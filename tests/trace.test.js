/*
 * 因果链追溯测试：从原始请求到所选消息、再到投递/确认/闭合的有序因果链。
 * 覆盖：
 *   - 合法重传闭合：dupOf 溯源、同世代失效与确认可追溯、重复投递仅回放既有动作；
 *   - 迟到确认：链路终止于首个违规步，并给出缺失/失配的等待关系；
 *   - 过期消息/重复投递越界/缺失拥有者数据：链终止与拒绝原因；
 *   - 部分确认：未闭合与缺失等待；
 *   - 选择入口：在途/已投递/被拒/重传副本均可列出；引用幽灵消息不伪造来源；
 *   - 追溯为纯派生：不修改回放证据。
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

function roles(chain) {
  return chain.links.map((l) => ({ role: l.role, step: l.step, id: l.msgId, ref: l.msgRefId, status: l.status }));
}

test('合法重传闭合：追溯重传副本 M6 可回到原件 M4 与同世代全部失效/确认', () => {
  const r = P.simulate(LEGAL_TRACE);
  const c = P.traceCausalChain(r, { msg: 6 });
  assert.equal(c.ok, true);
  assert.equal(c.status, 'closed');
  assert.equal(c.rootId, 4);
  assert.equal(c.selected.id, 6);
  assert.equal(c.selected.dupOf, 4);

  // 超时环显式关联原始消息
  const timeout = c.links.find((l) => l.role === 'timeout');
  assert.ok(timeout);
  assert.equal(timeout.step, 8);
  assert.equal(timeout.msgId, 4);
  assert.match(timeout.action, /M4 → M6/);

  // 同世代失效 M4/M5 都在链上（同世代失效与确认可追溯）
  const sends = c.links.filter((l) => l.role === 'send').map((l) => l.msgId);
  assert.deepEqual(sends, [4, 5, 6]);

  // 同世代两条确认与闭合
  const acks = c.links.filter((l) => l.role === 'ack');
  assert.equal(acks.length, 2);
  assert.deepEqual(acks.map((l) => l.core).sort(), [0, 2]);
  const close = c.links.find((l) => l.role === 'close');
  assert.ok(close);
  assert.equal(close.step, 13);
  assert.equal(close.gen, 1);
  assert.equal(c.closure.requester, 1);
  assert.equal(c.waiting, null);
});

test('合法重传闭合：重复投递环只能回放既有动作，不产生新来源', () => {
  const r = P.simulate(LEGAL_TRACE);
  const c = P.traceCausalChain(r, { msg: 6 });
  const replay = c.links.find((l) => l.role === 'deliver' && l.msgId === 6);
  assert.equal(replay.status, 'replay');
  assert.match(replay.action, /回放第 9 步 M4 的既有动作/);
  assert.match(replay.action, /不另造来源|不得/);
  // 重传副本不是新的失效来源：链上不存在 M6 专属确认（确认挂在原件 M4）
  const ackForM6 = c.links.find((l) => l.role === 'ack' && l.msgRefId === 6);
  assert.equal(ackForM6, undefined);
  const ackForM4 = c.links.find((l) => l.role === 'ack' && l.msgRefId === 4);
  assert.ok(ackForM4);
});

test('合法重传闭合：每一环都标明事件序号、消息号、缓存线、核心、世代与动作结果', () => {
  const r = P.simulate(LEGAL_TRACE);
  const c = P.traceCausalChain(r, { msg: 4 });
  for (const l of c.links) {
    assert.equal(typeof l.step, 'number');
    assert.ok(l.msgId !== null || l.msgIds !== null || l.msgRefId !== null);
    assert.equal(typeof l.line, 'number');
    assert.equal(typeof l.core, 'number');
    assert.equal(typeof l.gen, 'number');
    assert.equal(typeof l.action, 'string');
    assert.ok(l.action.length > 0);
    assert.equal(typeof l.result, 'string');
  }
  // 链按 (事件序号, 环内次序) 有序
  for (let i = 1; i < c.links.length; i++) {
    const a = c.links[i - 1]; const b = c.links[i];
    assert.ok(a.step < b.step || (a.step === b.step && a.order <= b.order));
  }
});

test('追溯入口：按投递/超时/确认事件步选择与按消息选择一致', () => {
  const r = P.simulate(LEGAL_TRACE);
  const byMsg = P.traceCausalChain(r, { msg: 4 });
  const byDeliver = P.traceCausalChain(r, { step: 9 }); // deliver M4
  const byTimeout = P.traceCausalChain(r, { step: 8 }); // timeout M4 → M6，解析到原件
  assert.equal(byDeliver.rootId, 4);
  assert.equal(byTimeout.rootId, 4);
  assert.deepEqual(roles(byMsg), roles(byDeliver));
});

test('迟到确认（世代不符）：从冻结步追溯，链终止于第 12 步并说明拒绝原因与失配等待', () => {
  const r = P.simulate(LATE_ACK_TRACE);
  const c = P.traceCausalChain(r, { step: 12 });
  assert.equal(c.status, 'rejected');
  assert.equal(c.terminalStep, 12);
  assert.equal(c.terminal.role, 'ack');
  assert.equal(c.rejection.kind, V.LATE_ACK);
  // 终止之后不再有环
  assert.ok(c.links.every((l) => l.step <= 12));
  assert.equal(c.links[c.links.length - 1].step, 12);
  // 缺失/失配的等待关系可见
  assert.match(c.waiting, /世代 2/);
  assert.match(c.waiting, /世代 1/);
  // 链首仍能看到新世代失效的原始请求与投递
  assert.equal(c.links[0].role, 'origin');
  assert.equal(c.links[0].gen, 2);
  assert.ok(c.links.some((l) => l.role === 'send' && l.msgId === 5 && l.gen === 2));
  assert.ok(c.links.some((l) => l.role === 'deliver' && l.msgId === 5));
  // 闭合信息缺失：迟到确认未闭合任何写入
  assert.equal(c.closure, null);
});

test('迟到确认：从导致冻结的在途消息 M5 追溯同样终止于违规确认', () => {
  const r = P.simulate(LATE_ACK_TRACE);
  const c = P.traceCausalChain(r, { msg: 5 });
  assert.equal(c.status, 'rejected');
  assert.equal(c.rejection.kind, V.LATE_ACK);
  assert.equal(c.terminalStep, 12);
});

test('迟到确认：旧世代链路本身仍可追溯其已完成的失效与闭合', () => {
  const r = P.simulate(LATE_ACK_TRACE);
  const c = P.traceCausalChain(r, { msg: 3 }); // 第一轮失效，已在第 7 步合法闭合
  assert.equal(c.status, 'closed');
  assert.equal(c.closure.step, 7);
  assert.ok(c.links.some((l) => l.role === 'ack' && l.step === 7));
});

test('迟到确认：目录无等待写入时，确认找不到既有失效来源（不伪造来源）', () => {
  const r = P.simulate({
    cores: 1, lines: 1,
    events: [{ type: 'ack', core: 0, line: 0, gen: 1 }],
  });
  const c = P.traceCausalChain(r, { step: 1 });
  assert.equal(c.status, 'rejected');
  assert.equal(c.selected, null);
  assert.equal(c.links.length, 1);
  assert.equal(c.links[0].status, 'bad');
  assert.equal(c.rejection.kind, V.LATE_ACK);
  assert.match(c.waiting, /无等待中的写入/);
  assert.match(c.links[0].result, /不伪造来源/);
});

test('迟到确认：确认者不在等待集合', () => {
  const r = P.simulate({
    cores: 3, lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'ack', core: 2, line: 0, gen: 1 },
    ],
  });
  const c = P.traceCausalChain(r, { step: 6 });
  assert.equal(c.status, 'rejected');
  assert.equal(c.rejection.kind, V.LATE_ACK);
  assert.match(c.waiting, /不在等待集合/);
});

test('重复确认：闭合后再次到达的确认只指向既有动作，链路终止于重复确认', () => {
  const r = P.simulate({
    cores: 2, lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'deliver', msg: 3 },
      { type: 'ack', core: 1, line: 0, gen: 1 },
      { type: 'ack', core: 1, line: 0, gen: 1 },
    ],
  });
  const c = P.traceCausalChain(r, { step: 8 });
  assert.equal(c.status, 'rejected');
  assert.equal(c.terminalStep, 8);
  assert.equal(c.rejection.kind, V.LATE_ACK);
  // 首次闭合仍在链上、终止步之前
  assert.ok(c.links.some((l) => l.role === 'close' && l.step === 7));
});

test('确认先于失效投递：等待关系中的确认来源缺失', () => {
  const r = P.simulate({
    cores: 2, lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'ack', core: 1, line: 0, gen: 1 },
    ],
  });
  const c = P.traceCausalChain(r, { step: 6 });
  assert.equal(c.status, 'rejected');
  assert.equal(c.rejection.kind, V.INVALID_EVENT);
  assert.match(c.waiting, /来源缺失|未登记/);
  // 失效 M3 尚未投递：链上无成功投递环
  assert.ok(!c.links.some((l) => l.role === 'deliver' && l.status === 'ok'));
});

test('重复失效越界：迟到重传副本的投递终止链路并拒绝', () => {
  const r = P.simulate({
    cores: 2, lines: 1,
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
      { type: 'deliver', msg: 4 },
    ],
  });
  const c = P.traceCausalChain(r, { msg: 4 });
  assert.equal(c.rootId, 3);
  assert.equal(c.status, 'rejected');
  assert.equal(c.terminalStep, 11);
  assert.equal(c.rejection.kind, V.STALE_MESSAGE);
  assert.equal(c.terminal.msgId, 4);
});

test('过期授权：世代落后的授权投递即终止', () => {
  const r = P.simulate({
    cores: 2, lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'deliver', msg: 3 },
      { type: 'deliver', msg: 2 },
    ],
  });
  const c = P.traceCausalChain(r, { msg: 2 });
  assert.equal(c.status, 'rejected');
  assert.equal(c.rejection.kind, V.STALE_MESSAGE);
  assert.equal(c.terminalStep, 6);
});

test('缺失拥有者数据：引用在途授权的违约环挂在链上', () => {
  const r = P.simulate({
    cores: 2, lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'read_miss', core: 1, line: 0 },
    ],
  });
  const c = P.traceCausalChain(r, { msg: 1 });
  assert.equal(c.status, 'rejected');
  assert.equal(c.rejection.kind, V.MISSING_OWNER_DATA);
  const refuse = c.links.find((l) => l.role === 'refuse');
  assert.ok(refuse);
  assert.equal(refuse.msgId, 1);
  assert.equal(refuse.step, 2);
});

test('引用从未出生的消息：显式标注缺失，绝不伪造来源', () => {
  const r = P.simulate({
    cores: 1, lines: 1,
    events: [{ type: 'deliver', msg: 99 }],
  });
  const c = P.traceCausalChain(r, { step: 1 });
  assert.equal(c.status, 'rejected');
  assert.equal(c.selected.missing, true);
  assert.equal(c.selected.id, 99);
  assert.equal(c.rootId, null);
  assert.match(c.links[0].action, /从未生成|已离开在途/);
  assert.match(c.links[0].result, /不伪造来源/);
});

test('部分确认：链路保持未闭合并指出缺失的确认等待', () => {
  const r = P.simulate({
    cores: 3, lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'read_miss', core: 2, line: 0 },
      { type: 'deliver', msg: 3 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'deliver', msg: 4 },
      { type: 'ack', core: 1, line: 0, gen: 1 },
    ],
  });
  const c = P.traceCausalChain(r, { msg: 5 }); // M5 失效→C2，未投递
  assert.equal(c.status, 'pending');
  assert.equal(c.closure, null);
  assert.match(c.waiting, /C2/);
  assert.match(c.waiting, /世代 1 确认缺失/);
  // 已确认的兄弟 M4 仍在链上
  assert.ok(c.links.some((l) => l.role === 'ack' && l.core === 1));
});

test('授权链：正常送达即闭合；未送达保持在途', () => {
  const done = P.simulate({
    cores: 1, lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
    ],
  });
  const cd = P.traceCausalChain(done, { msg: 1 });
  assert.equal(cd.status, 'closed');
  assert.equal(cd.closure.kind, 'grant');

  const inflight = P.simulate({
    cores: 1, lines: 1,
    events: [{ type: 'read_miss', core: 0, line: 0 }],
  });
  const ci = P.traceCausalChain(inflight, { msg: 1 });
  assert.equal(ci.status, 'pending');
  assert.match(ci.waiting, /仍在途/);
});

test('重传副本先于原件到达：首个到达者执行真实动作，后到原件只回放既有动作', () => {
  const r = P.simulate({
    cores: 2, lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 }, // M3 失效→C1
      { type: 'timeout', msg: 3 },                 // M4 重传副本
      { type: 'deliver', msg: 4 },                 // 副本先到：执行原件动作
      { type: 'deliver', msg: 3 },                 // 原件后到：只能回放
      { type: 'ack', core: 1, line: 0, gen: 1 },
    ],
  });
  const c = P.traceCausalChain(r, { msg: 3 });
  assert.equal(c.status, 'closed');
  const d4 = c.links.find((l) => l.role === 'deliver' && l.msgId === 4);
  const d3 = c.links.find((l) => l.role === 'deliver' && l.msgId === 3);
  assert.equal(d4.status, 'ok');
  assert.equal(d3.status, 'replay');
  assert.match(d3.action, /回放第 7 步 M4 的既有动作/);
  assert.match(d4.action, /先于原件到达/);
});

test('多级重传：每一级副本都溯源到同一原始消息', () => {  const r = P.simulate({
    cores: 2, lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 }, // M3 失效→C1
      { type: 'timeout', msg: 3 },                 // M4
      { type: 'timeout', msg: 4 },                 // M5（重传副本的再次重传）
    ],
  });
  for (const id of [3, 4, 5]) {
    const c = P.traceCausalChain(r, { msg: id });
    assert.equal(c.rootId, 3, `M${id} rootId`);
  }
  const c5 = P.traceCausalChain(r, { msg: 5 });
  const timeouts = c5.links.filter((l) => l.role === 'timeout');
  assert.equal(timeouts.length, 2);
  assert.deepEqual(timeouts.map((l) => l.step), [6, 7]);
});

test('消息清单：覆盖在途/已投递/被拒/重传副本并带溯源与出生步', () => {
  const r = P.simulate(LATE_ACK_TRACE);
  const list = P.listTraceableMessages(r);
  const ids = list.map((m) => m.id);
  assert.deepEqual(ids, [1, 2, 3, 4, 5]);
  const m5 = list.find((m) => m.id === 5);
  assert.equal(m5.kind, 'inv');
  assert.equal(m5.to, 1);
  assert.equal(m5.gen, 2);
  assert.equal(m5.state, 'rejected'); // 其投递后首个相关动作（迟到确认）违规
  assert.equal(m5.stateStep, 12);
  assert.equal(m5.inFlight, false);
});

test('重传副本在清单中标注 dupOf/rootId 与在途状态', () => {
  const r = P.simulate({
    cores: 2, lines: 1,
    events: [
      { type: 'read_miss', core: 0, line: 0 },
      { type: 'deliver', msg: 1 },
      { type: 'read_miss', core: 1, line: 0 },
      { type: 'deliver', msg: 2 },
      { type: 'write_upgrade', core: 0, line: 0 },
      { type: 'timeout', msg: 3 },
    ],
  });
  const list = P.listTraceableMessages(r);
  const m4 = list.find((m) => m.id === 4);
  assert.equal(m4.dupOf, 3);
  assert.equal(m4.rootId, 3);
  assert.equal(m4.state, 'in-flight');
  assert.equal(m4.inFlight, true);
});

test('追溯是对回放证据的纯派生：重复调用一致且不改动快照', () => {
  const r = P.simulate(LEGAL_TRACE);
  const before = JSON.stringify(r);
  const c1 = P.traceCausalChain(r, { msg: 6 });
  const c2 = P.traceCausalChain(r, { msg: 6 });
  assert.deepEqual(JSON.parse(JSON.stringify(c1)), JSON.parse(JSON.stringify(c2)));
  assert.equal(JSON.stringify(r), before);
  assert.deepEqual(P.listTraceableMessages(r).map((m) => m.id), [1, 2, 3, 4, 5, 6]);
  assert.equal(JSON.stringify(r), before);
});

test('非法/空结果被拒绝', () => {
  assert.equal(P.traceCausalChain(null, { msg: 1 }).ok, false);
  assert.equal(P.traceCausalChain({ ok: false, error: 'x' }, { msg: 1 }).ok, false);
  assert.deepEqual(P.listTraceableMessages(null), []);
});
