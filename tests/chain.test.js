/*
 * 因果链回溯测试。
 * 覆盖：
 *   - 合法重传闭合：从（重传）失效可追溯同世代失效、投递与确认直至闭合；
 *   - 超时重传经 dupOf 关联唯一原始消息，重复投递只回放既有动作；
 *   - 迟到确认：链路终止于首个违规步，并展示缺失/失配的等待关系；
 *   - 选择导致冻结的消息同样终止于首个违规步；
 *   - 链路纯由同一份回放证据派生（对旧结果查询不会被新结果污染）。
 */
'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const P = require('../app/protocol.js');

const V = P.VIOLATION;

const LEGAL_TRACE = {
  cores: 3,
  lines: 1,
  events: [
    { type: 'read_miss', core: 0, line: 0 },
    { type: 'deliver', msg: 1 },
    { type: 'read_miss', core: 1, line: 0 },
    { type: 'deliver', msg: 2 },
    { type: 'read_miss', core: 2, line: 0 },
    { type: 'deliver', msg: 3 },
    { type: 'write_upgrade', core: 1, line: 0, data: 7 },
    { type: 'timeout', msg: 4 },
    { type: 'deliver', msg: 4 },
    { type: 'deliver', msg: 6 },
    { type: 'ack', core: 0, line: 0, gen: 1 },
    { type: 'deliver', msg: 5 },
    { type: 'ack', core: 2, line: 0, gen: 1 },
  ],
};

const LATE_ACK_TRACE = {
  cores: 3,
  lines: 1,
  events: [
    { type: 'read_miss', core: 0, line: 0 },
    { type: 'deliver', msg: 1 },
    { type: 'read_miss', core: 1, line: 0 },
    { type: 'deliver', msg: 2 },
    { type: 'write_upgrade', core: 1, line: 0, data: 7 },
    { type: 'deliver', msg: 3 },
    { type: 'ack', core: 0, line: 0, gen: 1 },
    { type: 'read_miss', core: 2, line: 0 },
    { type: 'deliver', msg: 4 },
    { type: 'write_upgrade', core: 2, line: 0, data: 9 },
    { type: 'deliver', msg: 5 },
    { type: 'ack', core: 1, line: 0, gen: 1 },
  ],
};

function roleSteps(chain, role) {
  return chain.rings.filter((r) => r.role === role);
}

test('可回溯对象清单：消息含重传溯源，确认标注接受/拒绝', () => {
  const r = P.simulate(LEGAL_TRACE);
  const t = P.listTraceables(r);
  assert.equal(t.messages.length, 6); // M1..M6
  const m6 = t.messages.find((x) => x.msgId === 6);
  assert.equal(m6.dupOf, 4);
  assert.equal(m6.msgKind, 'inv');
  assert.equal(m6.gen, 1);
  assert.equal(t.messages.find((x) => x.msgId === 4).deliveredStep, 9);
  assert.equal(t.messages.find((x) => x.msgId === 6).deliveredStep, 10);
  assert.deepEqual(t.acks.map((a) => a.step), [11, 13]);
  assert.ok(t.acks.every((a) => !a.rejected));
});

test('合法重传闭合：从原始失效 M4 可追溯同世代失效与确认直至闭合', () => {
  const r = P.simulate(LEGAL_TRACE);
  const c = P.buildChain(r, { kind: 'message', msgId: 4 });
  assert.equal(c.ok, true);
  assert.equal(c.subject.rootMsgId, 4);
  assert.equal(c.terminatedByViolation, false);

  // 有序因果链：原始请求 → 超时重传 → 首次投递 → 重复投递回放 → 本核确认 → 同世代旁证 → 闭合
  assert.deepEqual(c.rings.map((x) => [x.step, x.role, x.msgId, x.core, x.peer || false]), [
    [7, 'origin', 4, 1, false],
    [8, 'retransmit', 6, 0, false],
    [9, 'deliver', 4, 0, false],
    [10, 'deliver-dup', 6, 0, false],
    [11, 'ack', null, 0, false],
    [12, 'deliver', 5, 2, true],
    [13, 'ack', null, 2, true],
  ]);
  const origin = c.rings.find((x) => x.role === 'origin');
  assert.equal(origin.step, 7);
  assert.equal(origin.msgId, 4);
  assert.equal(origin.line, 0);
  assert.equal(origin.core, 1);
  assert.equal(origin.gen, 1);
  assert.deepEqual(origin.alsoCreated, [5]);

  const retr = c.rings.find((x) => x.role === 'retransmit');
  assert.equal(retr.msgId, 6);
  assert.equal(retr.dupOf, 4);
  assert.equal(retr.step, 8);
  assert.equal(retr.gen, 1);
  assert.equal(retr.core, 0);

  const firstDeliver = c.rings.find((x) => x.role === 'deliver' && x.msgId === 4);
  assert.equal(firstDeliver.step, 9);
  assert.equal(firstDeliver.replayOf, null);

  // 重复投递 M6 只能回放 #9 的既有动作，不伪造新来源
  const dupDeliver = c.rings.find((x) => x.role === 'deliver-dup');
  assert.equal(dupDeliver.msgId, 6);
  assert.equal(dupDeliver.dupOf, 4);
  assert.equal(dupDeliver.replayOf, 9);
  assert.match(dupDeliver.action, /回放既有动作/);

  // 本核确认 + 同世代旁证（M5→C2 及其确认）
  const ownAcks = c.rings.filter((x) => x.role === 'ack' && !x.peer);
  assert.equal(ownAcks.length, 1);
  assert.deepEqual(ownAcks.map((x) => [x.step, x.core, x.gen]), [[11, 0, 1]]);
  const peerAcks = c.rings.filter((x) => x.role === 'ack' && x.peer);
  assert.deepEqual(peerAcks.map((x) => [x.step, x.core, x.gen]), [[13, 2, 1]]);
  assert.ok(c.rings.some((x) => x.peer && x.msgId === 5));

  const closer = c.rings.find((x) => x.closes);
  assert.equal(closer.step, 13);
  assert.match(c.endNote, /#13 闭合/);
});

test('从重传副本 M6 回看：主体标注来源，链路与从 M4 回看一致', () => {
  const r = P.simulate(LEGAL_TRACE);
  const c = P.buildChain(r, { kind: 'message', msgId: 6 });
  assert.equal(c.ok, true);
  assert.equal(c.subject.rootMsgId, 4);
  assert.equal(c.subject.dupOf, 4);
  const sel = c.rings.find((x) => x.selected);
  assert.equal(sel.msgId, 6);
  // 同一证据：超时环与两次投递都在
  assert.ok(c.rings.some((x) => x.role === 'retransmit' && x.msgId === 6 && x.dupOf === 4));
  assert.ok(c.rings.some((x) => x.msgId === 4 && x.role === 'deliver'));
  assert.ok(c.rings.some((x) => x.msgId === 6 && x.role === 'deliver-dup'));
});

test('合法重传闭合：从最后确认回看可追到原始写升级', () => {
  const r = P.simulate(LEGAL_TRACE);
  const c = P.buildChain(r, { kind: 'ack', step: 13 });
  assert.equal(c.ok, true);
  assert.equal(c.terminatedByViolation, false);
  assert.equal(c.relation.status, 'match');
  assert.deepEqual(c.relation.waitAcks, [2]); // 闭合前 C2 的确认被接受时集合里还剩它自己
  const origin = c.rings.find((x) => x.role === 'origin');
  assert.equal(origin.step, 7);
  assert.equal(origin.gen, 1);
  const ack = c.rings.find((x) => x.selected);
  assert.equal(ack.step, 13);
  assert.equal(ack.closes, true);
});

test('迟到确认（世代失配）：链路终止于首个违规步并说明等待关系', () => {
  const r = P.simulate(LATE_ACK_TRACE);
  const c = P.buildChain(r, { kind: 'ack', step: 12 });
  assert.equal(c.ok, true);
  assert.equal(c.terminatedByViolation, true);

  // 最后一环即违规确认，之后无任何环节
  const last = c.rings[c.rings.length - 1];
  assert.equal(last.step, 12);
  assert.equal(last.role, 'ack');
  assert.equal(last.violation.kind, V.LATE_ACK);
  assert.match(last.violation.detail, /世代 1/);
  assert.match(last.violation.detail, /世代 2/);
  assert.match(c.endNote, /#12/);

  // 缺失/失配的等待关系
  assert.equal(c.relation.status, 'gen-mismatch');
  assert.equal(c.relation.pendingGen, 2);
  assert.deepEqual(c.relation.waitAcks, [1]);
  assert.equal(c.relation.requester, 2);
  assert.match(c.relation.explain, /世代失配/);

  // 旧轮证据：第 1 世代写升级 #5 与其闭合确认 #7
  const oldOrigin = c.rings.find((x) => x.historical && x.role === 'origin');
  assert.equal(oldOrigin.step, 5);
  assert.equal(oldOrigin.gen, 1);
  const oldClose = c.rings.find((x) => x.historical && x.closes);
  assert.equal(oldClose.step, 7);
  assert.equal(c.relation.historical.closedAt, 7);

  // 当前等待轮的真实失效：M5→C1 已在 #11 投递
  const invInfo = c.relation.invs.find((x) => x.msgId === 5);
  assert.equal(invInfo.to, 1);
  assert.equal(invInfo.deliveredStep, 11);
});

test('迟到确认（无等待写入）：等待关系缺失，旧轮闭合证据仍可见', () => {
  const r = P.simulate({
    cores: 2,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'deliver', msg: 3 },
      { type: 'ack', core: 1, line: 0, gen: 1 },
      { type: 'ack', core: 1, line: 0, gen: 1 }, // 重复确认：目录已无等待
    ],
  });
  const c = P.buildChain(r, { kind: 'ack', step: 8 });
  assert.equal(c.terminatedByViolation, true);
  assert.equal(c.relation.status, 'no-pending');
  assert.match(c.relation.explain, /无等待/);
  assert.equal(c.relation.historical.closedAt, 7);
  const last = c.rings[c.rings.length - 1];
  assert.equal(last.violation.kind, V.LATE_ACK);
});

test('迟到确认（确认者不在等待集合）', () => {
  const r = P.simulate({
    cores: 3,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'ack', core: 2, line: 0, gen: 1 },
    ],
  });
  const c = P.buildChain(r, { kind: 'ack', step: 6 });
  assert.equal(c.relation.status, 'not-waiting');
  assert.deepEqual(c.relation.waitAcks, [1]);
  assert.equal(c.rings[c.rings.length - 1].violation.kind, V.LATE_ACK);
});

test('确认先于失效投递：缺失来源等待关系（核心未登记待确认）', () => {
  const r = P.simulate({
    cores: 2,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'ack', core: 1, line: 0, gen: 1 },
    ],
  });
  const c = P.buildChain(r, { kind: 'ack', step: 6 });
  assert.equal(c.terminatedByViolation, true);
  assert.equal(c.relation.status, 'no-owed-ack');
  // 当前轮失效 M3 在确认时尚未投递
  const invInfo = c.relation.invs.find((x) => x.msgId === 3);
  assert.equal(invInfo.deliveredStep, null);
});

test('选择导致冻结的消息：链路止于该消息的首个违规投递', () => {
  const r = P.simulate({
    cores: 2,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'timeout', msg: 3 },
      { type: 'deliver', msg: 3 },
      { type: 'ack', core: 1, line: 0, gen: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 5 },
      { type: 'deliver', msg: 4 }, // 迟到的重传失效：stale-message 冻结
    ],
  });
  const c = P.buildChain(r, { kind: 'message', msgId: 4 });
  assert.equal(c.terminatedByViolation, true);
  const last = c.rings[c.rings.length - 1];
  assert.equal(last.step, 11);
  assert.equal(last.msgId, 4);
  assert.equal(last.violation.kind, V.STALE_MESSAGE);
  assert.match(c.endNote, /首个违约步 #11/);
  // 冻结之前的旧轮闭合动作仍在链上
  assert.ok(c.rings.some((x) => x.role === 'retransmit' && x.msgId === 4 && x.dupOf === 3));
});

test('未投递即结束的失效：链路提示缺失确认', () => {
  const r = P.simulate({
    cores: 2,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 }, // M3 在途，记录结束
    ],
  });
  const c = P.buildChain(r, { kind: 'message', msgId: 3 });
  assert.equal(c.terminatedByViolation, false);
  assert.match(c.endNote, /仍在途/);
});

test('授权消息链路：无确认环节，重传副本同样溯源', () => {
  const r = P.simulate({
    cores: 2,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'timeout', msg: 1 },
      { type: 'deliver', msg: 1 },
    ],
  });
  const c = P.buildChain(r, { kind: 'message', msgId: 2 });
  assert.equal(c.subject.kind, 'grant');
  assert.equal(c.subject.dupOf, 1);
  assert.ok(c.rings.some((x) => x.role === 'retransmit' && x.dupOf === 1));
  assert.ok(!c.rings.some((x) => x.role === 'ack'));
  assert.match(c.endNote, /授权/);
});

test('非法查询被拒绝', () => {
  const r = P.simulate(LEGAL_TRACE);
  assert.equal(P.buildChain(r, { kind: 'message', msgId: 99 }).ok, false);
  assert.equal(P.buildChain(r, { kind: 'ack', step: 3 }).ok, false); // #3 不是确认
  assert.equal(P.buildChain(r, { kind: 'wat' }).ok, false);
  assert.equal(P.buildChain({ ok: false }, { kind: 'message', msgId: 1 }).ok, false);
});
