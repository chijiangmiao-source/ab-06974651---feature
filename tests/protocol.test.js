/*
 * 规则测试：世代绑定的失效/确认协议。
 * 覆盖：合法重传闭合、迟到确认拒绝、重复投递回放、缺失拥有者数据、
 *       过期消息、重复独占者、目录与副本不一致、输入上限。
 */
'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const P = require('../app/protocol.js');

const V = P.VIOLATION;

function lastStep(result) {
  return result.steps[result.steps.length - 1];
}

// 合法重传闭合轨迹：重传的失效请求在确认前送达，写入合法闭合
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

// 迟到确认轨迹：第一轮闭合后，旧世代确认在新一轮写入中到达
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

test('输入限制：核心数、缓存线数、事件数上限', () => {
  assert.equal(P.simulate({ cores: 5, lines: 1, events: [] }).ok, false);
  assert.equal(P.simulate({ cores: 0, lines: 1, events: [] }).ok, false);
  assert.equal(P.simulate({ cores: 1, lines: 9, events: [] }).ok, false);
  assert.equal(P.simulate({ cores: 1, lines: 1, events: null }).ok, false);
  const tooMany = { cores: 1, lines: 1, events: new Array(49).fill({ type: 'read_miss', core: 0, line: 0 }) };
  const r = P.simulate(tooMany);
  assert.equal(r.ok, false);
  assert.match(r.error, /48/);
});

test('输入限制：48 个事件恰好接受', () => {
  const events = [
    { type: 'read_miss', core: 0, line: 0 },
    { type: 'deliver', msg: 1 },
  ];
  for (let i = 0; i < 46; i++) events.push({ type: 'read_miss', core: 0, line: 0 }); // 命中，无动作
  assert.equal(events.length, 48);
  const r = P.simulate({ cores: 1, lines: 1, events });
  assert.equal(r.ok, true);
  assert.equal(r.violation, null);
  assert.equal(r.eventsProcessed, 48);
});

test('合法重传闭合：重传失效在确认前送达，写入闭合且无违约', () => {
  const r = P.simulate(LEGAL_TRACE);
  assert.equal(r.ok, true);
  assert.equal(r.violation, null);
  assert.equal(r.eventsProcessed, 13);
  assert.equal(r.steps.length, 14); // 初始状态 + 13 个事件

  const fin = lastStep(r);
  assert.equal(fin.dir[0].state, 'E');
  assert.equal(fin.dir[0].owner, 1);
  assert.deepEqual(fin.dir[0].sharers, [1]);
  assert.equal(fin.dir[0].gen, 1);
  assert.equal(fin.dir[0].pending, null);
  assert.equal(fin.caches[1].lines[0].state, 'M');
  assert.equal(fin.caches[1].lines[0].data, 7);
  assert.equal(fin.caches[0].lines[0].state, 'I');
  assert.equal(fin.caches[2].lines[0].state, 'I');
  assert.equal(fin.inFlight.length, 0);

  // 第 10 步（投递重传副本 M6）只能回放既有动作
  const dupStep = r.steps[10];
  assert.equal(dupStep.event.type, 'deliver');
  assert.equal(dupStep.event.msg, 6);
  assert.match(dupStep.note, /回放既有动作/);
  assert.equal(dupStep.caches[0].lines[0].state, 'I');
  assert.equal(dupStep.caches[0].lines[0].owedAckGen, 1);
});

test('失效请求绑定递增世代', () => {
  const r = P.simulate(LATE_ACK_TRACE);
  // 第一轮写升级后世代为 1
  const round1 = r.steps[5];
  assert.equal(round1.dir[0].gen, 1);
  const inv1 = round1.inFlight.find((m) => m.kind === 'inv');
  assert.equal(inv1.gen, 1);
  // 第二轮写升级（第 10 步）世代递增为 2，且冻结前保持等待
  const round2 = r.steps[10];
  assert.equal(round2.dir[0].gen, 2);
  const inv2 = round2.inFlight.find((m) => m.kind === 'inv');
  assert.equal(inv2.gen, 2);
});

test('迟到确认拒绝：旧世代确认不得闭合新一轮写入，首次违约步冻结', () => {
  const r = P.simulate(LATE_ACK_TRACE);
  assert.equal(r.ok, true);
  assert.equal(r.eventsProcessed, 12); // 在第 12 个事件冻结
  assert.equal(r.steps.length, 13);

  const v = r.violation;
  assert.equal(v.kind, V.LATE_ACK);
  assert.equal(v.step, 12);
  assert.equal(v.line, 0);
  assert.equal(v.core, 1);
  assert.match(v.detail, /世代 1/);
  assert.match(v.detail, /世代 2|等待世代 2/);

  // 冻结时独占未被错误释放：目录仍在等待世代 2，请求者尚未获得 M
  const fin = lastStep(r);
  assert.equal(fin.dir[0].pending.gen, 2);
  assert.deepEqual(fin.dir[0].pending.waitAcks, [1]);
  assert.equal(fin.caches[2].lines[0].state, 'S');
  assert.equal(fin.dir[0].owner, null);
});

test('迟到确认拒绝：目录无等待中的写入', () => {
  const r = P.simulate({
    cores: 1,
    lines: 1,
    events: [{ type: 'ack', core: 0, line: 0, gen: 1 }],
  });
  const v = r.violation;
  assert.equal(v.kind, V.LATE_ACK);
  assert.equal(v.step, 1);
  assert.match(v.detail, /无等待中的写入/);
});

test('迟到确认拒绝：确认者不在等待集合', () => {
  const r = P.simulate({
    cores: 3,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 }, // 等待 {C1}
      { type: 'ack', core: 2, line: 0, gen: 1 },   // C2 与本轮无关
    ],
  });
  const v = r.violation;
  assert.equal(v.kind, V.LATE_ACK);
  assert.equal(v.step, 6);
  assert.equal(v.core, 2);
  assert.match(v.detail, /不在.*等待集合/);
});

test('迟到确认拒绝：重复确认（已接受过的确认再次到达）', () => {
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
      { type: 'ack', core: 1, line: 0, gen: 1 }, // 接受，写入闭合
      { type: 'ack', core: 1, line: 0, gen: 1 }, // 重复确认：目录已无等待
    ],
  });
  assert.equal(r.violation.kind, V.LATE_ACK);
  assert.equal(r.violation.step, 8);
});

test('确认先于失效投递：确认来源不明', () => {
  const r = P.simulate({
    cores: 2,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'ack', core: 1, line: 0, gen: 1 }, // C1 尚未收到失效请求
    ],
  });
  assert.equal(r.violation.kind, V.INVALID_EVENT);
  assert.match(r.violation.detail, /确认来源不明|未登记/);
});

test('缺失拥有者数据：授权在途时另一核心读缺失', () => {
  const r = P.simulate({
    cores: 2,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 }, // 生成 M1（E 授权），未投递
      { type: 'read_miss', core: 1, line: 0 }, // 需要 C0 数据，但 C0 副本仍为 I
    ],
  });
  const v = r.violation;
  assert.equal(v.kind, V.MISSING_OWNER_DATA);
  assert.equal(v.step, 2);
  assert.equal(v.line, 0);
  assert.equal(v.core, 0);
  assert.equal(v.msgId, 1);
  assert.match(v.detail, /M1.*在途|在途/);
});

test('过期数据授权：授权世代落后于核心已见失效世代', () => {
  const r = P.simulate({
    cores: 2,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 }, // M2 授权 S→C1（世代 0），暂不投递
      { type: 'write_upgrade', core: 0, line: 0 }, // 世代 1，M3 失效→C1
      { type: 'deliver', msg: 3 },             // C1 已见世代 1
      { type: 'deliver', msg: 2 },             // 世代 0 的授权到达：过期
    ],
  });
  const v = r.violation;
  assert.equal(v.kind, V.STALE_MESSAGE);
  assert.equal(v.step, 6);
  assert.equal(v.msgId, 2);
  assert.equal(v.core, 1);
});

test('重复失效投递不得产生新动作：核心已重新持有副本', () => {
  const r = P.simulate({
    cores: 2,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 }, // 世代 1，M3 失效→C1
      { type: 'timeout', msg: 3 },                 // M4 = M3 的重传副本
      { type: 'deliver', msg: 3 },
      { type: 'ack', core: 1, line: 0, gen: 1 },   // 写入闭合，C0 获得 M
      { type: 'read_miss', core: 1, line: 0 },     // M5 授权 S→C1
      { type: 'deliver', msg: 5 },                 // C1 重新持有 S
      { type: 'deliver', msg: 4 },                 // 迟到的重传失效：不得再次失效 C1
    ],
  });
  const v = r.violation;
  assert.equal(v.kind, V.STALE_MESSAGE);
  assert.equal(v.step, 11);
  assert.equal(v.msgId, 4);
  assert.match(v.detail, /重新持有副本/);
});

test('写入闭合前不释放独占：部分确认后请求者仍为 S', () => {
  const r = P.simulate({
    cores: 3,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'read_miss', core: 2, line: 0 },
      { type: 'deliver', msg: 3 },
      { type: 'write_upgrade', core: 0, line: 0 }, // 等待 {C1,C2}
      { type: 'deliver', msg: 4 },
      { type: 'ack', core: 1, line: 0, gen: 1 },   // 仅 C1 确认
    ],
  });
  assert.equal(r.violation, null);
  const fin = lastStep(r);
  assert.deepEqual(fin.dir[0].pending.waitAcks, [2]);
  assert.equal(fin.dir[0].state, 'S');            // 目录尚未切换为独占
  assert.equal(fin.caches[0].lines[0].state, 'S'); // 请求者尚未升级
});

test('重复独占者检测（状态检查器）', () => {
  const s = P.createState(2, 1);
  s.caches[0].lines[0].state = 'E';
  s.caches[1].lines[0].state = 'M';
  const v = P.checkConsistency(s);
  assert.equal(v.kind, V.DUPLICATE_EXCLUSIVE);
  assert.equal(v.line, 0);
  assert.deepEqual(v.core, [0, 1]);
});

test('目录与副本不一致检测（状态检查器）', () => {
  // 目录记录共享者 C0，但 C0 无副本
  const s1 = P.createState(2, 1);
  s1.dir[0].state = 'S';
  s1.dir[0].sharers = [0];
  const v1 = P.checkConsistency(s1);
  assert.equal(v1.kind, V.DIR_COPY_MISMATCH);
  assert.equal(v1.line, 0);

  // 目录为 I，但核心持有副本
  const s2 = P.createState(2, 1);
  s2.caches[1].lines[0].state = 'S';
  const v2 = P.checkConsistency(s2);
  assert.equal(v2.kind, V.DIR_COPY_MISMATCH);
  assert.deepEqual(v2.core, [1]);

  // 目录独占者 C0，但 C0 无副本
  const s3 = P.createState(2, 1);
  s3.dir[0].state = 'E';
  s3.dir[0].owner = 0;
  s3.dir[0].sharers = [0];
  const v3 = P.checkConsistency(s3);
  assert.equal(v3.kind, V.DIR_COPY_MISMATCH);
});

test('非法事件：投递/重传不存在的消息', () => {
  const r1 = P.simulate({ cores: 1, lines: 1, events: [{ type: 'deliver', msg: 99 }] });
  assert.equal(r1.violation.kind, V.INVALID_EVENT);
  const r2 = P.simulate({ cores: 1, lines: 1, events: [{ type: 'timeout', msg: 99 }] });
  assert.equal(r2.violation.kind, V.INVALID_EVENT);
});

test('非法事件：未知类型与越界引用', () => {
  const r1 = P.simulate({ cores: 1, lines: 1, events: [{ type: 'nuke', core: 0, line: 0 }] });
  assert.equal(r1.violation.kind, V.INVALID_EVENT);
  const r2 = P.simulate({ cores: 1, lines: 1, events: [{ type: 'read_miss', core: 3, line: 0 }] });
  assert.equal(r2.violation.kind, V.INVALID_EVENT);
  const r3 = P.simulate({ cores: 1, lines: 1, events: [{ type: 'read_miss', core: 0, line: 7 }] });
  assert.equal(r3.violation.kind, V.INVALID_EVENT);
});

test('超时重传：复制在途消息且字段一致、编号递增', () => {
  const r = P.simulate({
    cores: 2,
    lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 }, // M3 失效→C1
      { type: 'timeout', msg: 3 },                 // M4 = 重传副本
    ],
  });
  assert.equal(r.violation, null);
  const fin = lastStep(r);
  const dup = fin.inFlight.find((m) => m.id === 4);
  assert.ok(dup);
  assert.equal(dup.kind, 'inv');
  assert.equal(dup.to, 1);
  assert.equal(dup.gen, 1);
  assert.equal(dup.dupOf, 3);
});

test('每步快照保留目录、副本、在途消息与世代号', () => {
  const r = P.simulate(LEGAL_TRACE);
  for (const s of r.steps) {
    assert.ok(Array.isArray(s.dir));
    assert.ok(Array.isArray(s.caches));
    assert.ok(Array.isArray(s.inFlight));
    for (const d of s.dir) assert.equal(typeof d.gen, 'number');
    for (const c of s.caches) {
      for (const cl of c.lines) assert.equal(typeof cl.lastInvGen, 'number');
    }
  }
  // 快照相互独立：修改后步不影响前步
  const a = r.steps[7];
  const b = r.steps[8];
  assert.notEqual(a, b);
  assert.equal(a.inFlight.length, 2); // M4、M5 在途
  assert.equal(b.inFlight.length, 3); // 超时重传后 M4、M5、M6
});
