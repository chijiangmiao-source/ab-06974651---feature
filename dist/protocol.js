/*
 * 目录缓存一致性重放核心（世代绑定的失效/确认协议）。
 *
 * 同一份代码运行在两个环境：
 *   - 浏览器 Web Worker（importScripts('protocol.js') 后挂到 self.Protocol）
 *   - Node.js（require，用于规则测试）
 *
 * 协议要点：
 *   - 每轮写升级让目录为该缓存线推进一个递增世代（gen），
 *     本轮所有失效请求（inv 消息）都绑定该世代；
 *   - 只有当前等待集合（waitAcks）中的同世代确认（ack）才能推进写入；
 *     迟到确认（无等待写入 / 世代不符 / 不在等待集合）在首次违约步冻结；
 *   - 超时重传只复制在途消息；重复投递只能回放既有动作（重新登记同世代
 *     待确认），不得产生新动作，否则按过期消息冻结；
 *   - 每步之后做全局一致性检查：重复独占者、目录与副本不一致。
 */
(function (root, factory) {
  const api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.Protocol = api;
})(typeof self !== 'undefined' ? self : globalThis, function () {
  'use strict';

  const LIMITS = Object.freeze({ maxCores: 4, maxLines: 8, maxEvents: 48 });

  const VIOLATION = Object.freeze({
    LATE_ACK: 'late-ack',
    MISSING_OWNER_DATA: 'missing-owner-data',
    DUPLICATE_EXCLUSIVE: 'duplicate-exclusive',
    DIR_COPY_MISMATCH: 'directory-copy-mismatch',
    STALE_MESSAGE: 'stale-message',
    INVALID_EVENT: 'invalid-event',
  });

  const VIOLATION_LABELS = Object.freeze({
    'late-ack': '迟到确认',
    'missing-owner-data': '缺失拥有者数据',
    'duplicate-exclusive': '重复独占者',
    'directory-copy-mismatch': '目录与副本不一致',
    'stale-message': '过期消息（重复投递越界）',
    'invalid-event': '非法事件',
  });

  const EVENT_TYPES = new Set(['read_miss', 'write_upgrade', 'deliver', 'timeout', 'ack']);

  function clone(x) { return structuredClone(x); }

  function cores(list) { return list.map((c) => 'C' + c).join(','); }

  // ---------- 状态 ----------

  function createState(numCores, numLines) {
    const dir = [];
    for (let l = 0; l < numLines; l++) {
      dir.push({
        line: l,
        state: 'I',          // 目录状态：I / S / E
        owner: null,         // E 时的独占拥有者
        sharers: [],         // 共享者集合（E 时等于 [owner]）
        data: 0,             // 目录（内存）已知数据
        gen: 0,              // 当前世代：每轮失效递增
        pending: null,       // { requester, gen, waitAcks:[], writeData }
      });
    }
    const caches = [];
    for (let c = 0; c < numCores; c++) {
      const lines = [];
      for (let l = 0; l < numLines; l++) {
        lines.push({
          state: 'I',        // 副本状态：I / S / E / M
          data: null,
          lastInvGen: 0,     // 本核心已见的最新失效世代
          owedAckGen: null,  // 已收失效、尚未送达目录的确认世代
        });
      }
      caches.push({ core: c, lines });
    }
    return { dir, caches, inFlight: [], nextMsgId: 1 };
  }

  function send(state, fields) {
    const m = Object.assign({ id: state.nextMsgId++, from: 'D', dupOf: null }, fields);
    state.inFlight.push(m);
    return m;
  }

  // ---------- 输入校验 ----------

  function validateInput(input) {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
      return '输入必须是 JSON 对象：{"cores":N,"lines":M,"events":[...]}';
    }
    const { cores: n, lines: l, events } = input;
    if (!Number.isInteger(n) || n < 1 || n > LIMITS.maxCores) {
      return `核心数须为 1..${LIMITS.maxCores} 的整数`;
    }
    if (!Number.isInteger(l) || l < 1 || l > LIMITS.maxLines) {
      return `缓存线数须为 1..${LIMITS.maxLines} 的整数`;
    }
    if (!Array.isArray(events)) return 'events 必须是数组';
    if (events.length > LIMITS.maxEvents) return `事件数 ${events.length} 超过上限 ${LIMITS.maxEvents}`;
    return null;
  }

  function validateEvent(ev, numCores, numLines) {
    if (!ev || typeof ev !== 'object' || !EVENT_TYPES.has(ev.type)) {
      return `未知事件类型（须为 ${[...EVENT_TYPES].join(' / ')}）`;
    }
    const needCoreLine = ev.type === 'read_miss' || ev.type === 'write_upgrade' || ev.type === 'ack';
    if (needCoreLine) {
      if (!Number.isInteger(ev.core) || ev.core < 0 || ev.core >= numCores) return '核心编号越界';
      if (!Number.isInteger(ev.line) || ev.line < 0 || ev.line >= numLines) return '缓存线编号越界';
    }
    if (ev.type === 'write_upgrade' && ev.data !== undefined && typeof ev.data !== 'number') {
      return '写数据 data 须为数字';
    }
    if ((ev.type === 'deliver' || ev.type === 'timeout') && !Number.isInteger(ev.msg)) {
      return '消息编号 msg 须为整数';
    }
    if (ev.type === 'ack' && !Number.isInteger(ev.gen)) return '确认世代 gen 须为整数';
    return null;
  }

  // ---------- 违约构造 ----------

  function violation(kind, line, core, msgId, detail) {
    return { kind, line, core, msgId, detail };
  }

  // ---------- 事件处理（返回 { note } 或 { note, violation }） ----------

  function ok(note) { return { note }; }
  function bad(note, v) { return { note, violation: v }; }

  function onReadMiss(state, ev) {
    const { core, line } = ev;
    const d = state.dir[line];
    const cl = state.caches[core].lines[line];
    if (cl.state !== 'I') return ok(`C${core} 在 L${line} 命中（${cl.state}），无需动作`);
    if (d.pending) {
      return bad('目录忙，读缺失无法受理', violation(
        VIOLATION.INVALID_EVENT, line, core, null,
        `L${line} 目录正等待第 ${d.pending.gen} 世代确认（等待集合 {${cores(d.pending.waitAcks)}}），读缺失无法受理`,
      ));
    }
    const inflightGrant = state.inFlight.find((m) => m.kind === 'grant' && m.to === core && m.line === line);
    if (inflightGrant) return ok(`授权消息 M${inflightGrant.id} 已在途，读缺失合并，无需重复授权`);

    if (d.state === 'I') {
      d.state = 'E'; d.owner = core; d.sharers = [core];
      const m = send(state, { kind: 'grant', to: core, line, gen: d.gen, grantState: 'E', data: d.data });
      return ok(`L${line} 目录空闲 → 授予 C${core} 独占（E），生成授权消息 M${m.id}（世代 ${d.gen}，数据=${d.data}）`);
    }
    if (d.state === 'S') {
      if (!d.sharers.includes(core)) d.sharers.push(core);
      const m = send(state, { kind: 'grant', to: core, line, gen: d.gen, grantState: 'S', data: d.data });
      return ok(`L${line} 共享态 → C${core} 加入共享集合 {${cores(d.sharers)}}，生成授权消息 M${m.id}（S，数据=${d.data}）`);
    }
    // d.state === 'E'
    const owner = d.owner;
    if (owner === core) return ok(`C${core} 已是 L${line} 独占者（授权在途），无需动作`);
    const oc = state.caches[owner].lines[line];
    if (oc.state === 'I' || oc.data === null) {
      const g = state.inFlight.find((m) => m.kind === 'grant' && m.to === owner && m.line === line);
      return bad('读缺失需要拥有者数据，但拥有者副本缺失', violation(
        VIOLATION.MISSING_OWNER_DATA, line, owner, g ? g.id : null,
        `L${line} 读缺失需要拥有者 C${owner} 的数据，但其副本为 ${oc.state}（数据=${oc.data}）`
        + (g ? `；授权消息 M${g.id} 仍在途，数据尚未到达` : ''),
      ));
    }
    d.data = oc.data; // 写回目录
    oc.state = 'S';
    d.state = 'S'; d.owner = null;
    if (!d.sharers.includes(core)) d.sharers.push(core);
    const m = send(state, { kind: 'grant', to: core, line, gen: d.gen, grantState: 'S', data: d.data });
    return ok(`拥有者 C${owner} 写回数据=${d.data} 并降级为 S；C${core} 加入共享集合 {${cores(d.sharers)}}，生成授权消息 M${m.id}`);
  }

  function onWriteUpgrade(state, ev) {
    const { core, line, data } = ev;
    const d = state.dir[line];
    const cl = state.caches[core].lines[line];
    if (d.pending) {
      return bad('目录忙，写升级无法受理', violation(
        VIOLATION.INVALID_EVENT, line, core, null,
        `L${line} 目录正等待第 ${d.pending.gen} 世代确认（等待集合 {${cores(d.pending.waitAcks)}}），写升级无法受理`,
      ));
    }
    if (cl.state === 'M') return ok(`C${core} 已持有 M，无需升级`);
    if (cl.state === 'E') {
      cl.state = 'M';
      if (data !== undefined) cl.data = data;
      return ok(`C${core} 静默升级 E→M（无其他副本，无需失效）`);
    }
    if (cl.state === 'I') {
      return bad('写升级时核心无副本', violation(
        VIOLATION.INVALID_EVENT, line, core, null,
        `写升级要求 C${core} 持有 L${line} 的 S/E 副本，实际为 I`,
      ));
    }
    // cl.state === 'S'：发起新一轮失效，世代递增
    const others = d.sharers.filter((x) => x !== core);
    d.gen += 1;
    const writeData = data !== undefined ? data : cl.data;
    if (others.length === 0) {
      cl.state = 'M'; cl.data = writeData;
      d.state = 'E'; d.owner = core; d.sharers = [core];
      return ok(`无其他共享者，写入立即闭合：C${core} 获得独占（M，世代 ${d.gen}，数据=${writeData}）`);
    }
    d.pending = { requester: core, gen: d.gen, waitAcks: others.slice(), writeData };
    const parts = [];
    for (const o of others) {
      const m = send(state, { kind: 'inv', to: o, line, gen: d.gen });
      parts.push(`M${m.id}→C${o}`);
    }
    return ok(`发起第 ${d.gen} 世代失效：${parts.join('，')}；等待确认集合 {${cores(others)}}`);
  }

  function onDeliver(state, ev) {
    const idx = state.inFlight.findIndex((m) => m.id === ev.msg);
    if (idx < 0) {
      return bad('投递的消息不存在', violation(
        VIOLATION.INVALID_EVENT, null, null, ev.msg,
        `消息 M${ev.msg} 不在在途集合（可能已投递或从未生成）`,
      ));
    }
    const m = state.inFlight.splice(idx, 1)[0];
    const cl = state.caches[m.to].lines[m.line];
    const dupTag = m.dupOf !== null ? `（M${m.dupOf} 的重传副本）` : '';

    if (m.kind === 'inv') {
      if (m.gen < cl.lastInvGen) {
        return bad('过期失效请求到达', violation(
          VIOLATION.STALE_MESSAGE, m.line, m.to, m.id,
          `失效请求 M${m.id}${dupTag}（世代 ${m.gen}）到达 C${m.to}，但核心已见世代 ${cl.lastInvGen}；重复投递只能回放既有动作`,
        ));
      }
      if (m.gen === cl.lastInvGen && cl.state === 'I') {
        cl.owedAckGen = m.gen; // 回放既有动作：重新登记同世代待确认
        return ok(`M${m.id}${dupTag} 为重复失效（世代 ${m.gen}）：回放既有动作，C${m.to} 重新登记待确认`);
      }
      if (m.gen === cl.lastInvGen && cl.state !== 'I') {
        return bad('重复失效请求越界', violation(
          VIOLATION.STALE_MESSAGE, m.line, m.to, m.id,
          `失效请求 M${m.id}${dupTag}（世代 ${m.gen}）到达 C${m.to}，但核心已在新一轮重新持有副本（${cl.state}）；重复投递不得产生新动作`,
        ));
      }
      const had = cl.state;
      cl.state = 'I'; cl.data = null;
      cl.lastInvGen = m.gen; cl.owedAckGen = m.gen;
      return ok(`C${m.to} 收到失效请求 M${m.id}${dupTag}（世代 ${m.gen}）：副本 ${had}→I，登记待确认`);
    }

    // m.kind === 'grant'
    if (m.gen < cl.lastInvGen) {
      return bad('过期数据授权到达', violation(
        VIOLATION.STALE_MESSAGE, m.line, m.to, m.id,
        `数据授权 M${m.id}${dupTag}（世代 ${m.gen}）到达 C${m.to}，但核心已见失效世代 ${cl.lastInvGen}；过期授权必须丢弃`,
      ));
    }
    const dup = cl.state !== 'I';
    cl.state = m.grantState; cl.data = m.data;
    return ok(dup
      ? `M${m.id}${dupTag} 为重复授权：回放既有动作，C${m.to} 保持 ${m.grantState}（数据=${m.data}）`
      : `C${m.to} 收到授权 M${m.id}${dupTag}：副本 → ${m.grantState}（数据=${m.data}）`);
  }

  function onTimeout(state, ev) {
    const m = state.inFlight.find((x) => x.id === ev.msg);
    if (!m) {
      return bad('重传的消息不存在', violation(
        VIOLATION.INVALID_EVENT, null, null, ev.msg,
        `消息 M${ev.msg} 不在在途集合，无法超时重传`,
      ));
    }
    const dup = Object.assign({}, m, { id: state.nextMsgId++, dupOf: m.id });
    state.inFlight.push(dup);
    return ok(`超时重传：复制 M${m.id} → M${dup.id}（${m.kind}，线 L${m.line}，世代 ${m.gen}）`);
  }

  function onAck(state, ev) {
    const { core, line, gen } = ev;
    const d = state.dir[line];
    const cl = state.caches[core].lines[line];
    if (!d.pending) {
      return bad('确认到达时目录无等待中的写入', violation(
        VIOLATION.LATE_ACK, line, core, null,
        `C${core} 对 L${line} 的确认（世代 ${gen}）到达时，目录无等待中的写入；旧确认不得释放新独占`,
      ));
    }
    if (d.pending.gen !== gen) {
      return bad('确认世代不符', violation(
        VIOLATION.LATE_ACK, line, core, null,
        `C${core} 对 L${line} 的确认世代 ${gen} 与当前等待世代 ${d.pending.gen} 不符；旧失效确认不得闭合新一轮写入`,
      ));
    }
    if (!d.pending.waitAcks.includes(core)) {
      return bad('确认者不在等待集合', violation(
        VIOLATION.LATE_ACK, line, core, null,
        `C${core} 不在 L${line} 当前等待集合 {${cores(d.pending.waitAcks)}} 中（可能重复确认或与本轮无关）`,
      ));
    }
    if (cl.owedAckGen !== gen) {
      return bad('核心未欠该世代确认', violation(
        VIOLATION.INVALID_EVENT, line, core, null,
        `C${core} 未登记世代 ${gen} 的待确认失效（当前待确认=${cl.owedAckGen}），确认来源不明`,
      ));
    }
    cl.owedAckGen = null;
    d.pending.waitAcks = d.pending.waitAcks.filter((x) => x !== core);
    if (d.pending.waitAcks.length === 0) {
      const req = d.pending.requester;
      const rcl = state.caches[req].lines[line];
      rcl.state = 'M'; rcl.data = d.pending.writeData;
      d.state = 'E'; d.owner = req; d.sharers = [req];
      const g = d.pending.gen;
      d.pending = null;
      return ok(`C${core} 的世代 ${gen} 确认接受；等待集合清空，写入闭合：C${req} 获得独占（M，世代 ${g}，数据=${rcl.data}）`);
    }
    return ok(`C${core} 的世代 ${gen} 确认接受；仍等待 {${cores(d.pending.waitAcks)}}`);
  }

  function applyEvent(state, ev) {
    switch (ev.type) {
      case 'read_miss': return onReadMiss(state, ev);
      case 'write_upgrade': return onWriteUpgrade(state, ev);
      case 'deliver': return onDeliver(state, ev);
      case 'timeout': return onTimeout(state, ev);
      case 'ack': return onAck(state, ev);
      default: return bad('未知事件', violation(VIOLATION.INVALID_EVENT, null, null, null, `未知事件类型 ${ev.type}`));
    }
  }

  // ---------- 全局一致性检查 ----------

  // 有效持有 = 缓存副本 + 将要生效的在途授权（世代不落后于核心已见失效世代）
  function effectiveHolders(state, line) {
    const holders = new Map(); // core -> 'S'|'E'|'M'
    for (const c of state.caches) {
      const cl = c.lines[line];
      if (cl.state !== 'I') holders.set(c.core, cl.state);
    }
    for (const m of state.inFlight) {
      if (m.kind === 'grant' && m.line === line) {
        const cl = state.caches[m.to].lines[line];
        if (m.gen >= cl.lastInvGen) holders.set(m.to, m.grantState);
      }
    }
    return holders;
  }

  function fmtHolders(holders) {
    return [...holders.entries()].map(([c, s]) => `C${c}:${s}`).join('，') || '（无）';
  }

  function checkConsistency(state) {
    for (const d of state.dir) {
      const line = d.line;
      const holders = effectiveHolders(state, line);
      const excl = [...holders.entries()].filter(([, s]) => s === 'E' || s === 'M');

      if (excl.length >= 2) {
        return violation(
          VIOLATION.DUPLICATE_EXCLUSIVE, line, excl.map(([c]) => c), null,
          `L${line} 出现重复独占者：${fmtHolders(new Map(excl))}；独占权限被错误释放`,
        );
      }
      if (excl.length === 1 && holders.size > 1) {
        return violation(
          VIOLATION.DIR_COPY_MISMATCH, line, excl[0][0], null,
          `L${line} 独占副本 ${fmtHolders(new Map(excl))} 与其他副本共存：${fmtHolders(holders)}`,
        );
      }
      if (d.pending) continue; // 写入进行中：目录处于过渡态，仅做独占性检查

      if (d.state === 'I' && holders.size > 0) {
        return violation(
          VIOLATION.DIR_COPY_MISMATCH, line, [...holders.keys()], null,
          `L${line} 目录为 I，但存在副本：${fmtHolders(holders)}`,
        );
      }
      if (d.state === 'S') {
        const bad = [...holders.entries()].filter(([, s]) => s !== 'S');
        const sameSet = holders.size === d.sharers.length && d.sharers.every((c) => holders.has(c));
        if (bad.length > 0 || !sameSet) {
          return violation(
            VIOLATION.DIR_COPY_MISMATCH, line, [...holders.keys()], null,
            `L${line} 目录共享者 {${cores(d.sharers)}} 与副本 ${fmtHolders(holders)} 不一致`,
          );
        }
      }
      if (d.state === 'E') {
        const okE = holders.size === 1 && holders.has(d.owner)
          && (holders.get(d.owner) === 'E' || holders.get(d.owner) === 'M');
        if (!okE) {
          return violation(
            VIOLATION.DIR_COPY_MISMATCH, line, d.owner, null,
            `L${line} 目录独占者 C${d.owner} 与副本 ${fmtHolders(holders)} 不一致`,
          );
        }
      }
    }
    return null;
  }

  // ---------- 因果链回溯 ----------
  //
  // 审查员从一条在途消息（或一条确认）出发，回看其产生与后续作用。
  // 链路完全由 simulate() 已固定的逐步证据派生，不重新执行协议，因此：
  //   - 重传副本经 dupOf 回到唯一原始消息，超时环只记录“复制”，不伪造来源；
  //   - 重复投递环通过 replayOf 指向最早生效的那次既有投递动作；
  //   - 迟到确认 / 导致冻结的消息，链路止于首个违约步并附拒绝原因；
  //   - 迟到确认同时给出其缺失或失配的等待关系（当前等待集合 + 旧轮闭合证据）。

  const CHAIN_ROLE_LABELS = Object.freeze({
    origin: '原始请求',
    retransmit: '超时重传',
    deliver: '投递·生效动作',
    'deliver-dup': '重复投递·回放既有动作',
    ack: '确认',
  });

  function buildTraceIndex(result) {
    const steps = result.steps;
    const messages = new Map(); // id -> { id, kind, to, line, gen, dupOf, createdAt }
    for (let i = 1; i < steps.length; i++) {
      const prev = new Set(steps[i - 1].inFlight.map((m) => m.id));
      for (const m of steps[i].inFlight) {
        if (!prev.has(m.id) && !messages.has(m.id)) {
          messages.set(m.id, {
            id: m.id, kind: m.kind, to: m.to, line: m.line, gen: m.gen,
            dupOf: m.dupOf, createdAt: i,
          });
        }
      }
    }
    const deliveries = new Map(); // 消息号 -> 投递事件步（含失败投递）
    const acks = [];
    for (let i = 1; i < steps.length; i++) {
      const ev = steps[i].event;
      if (!ev) continue;
      if (ev.type === 'deliver') {
        const list = deliveries.get(ev.msg) || [];
        list.push(i);
        deliveries.set(ev.msg, list);
      } else if (ev.type === 'ack') {
        acks.push({ step: i, core: ev.core, line: ev.line, gen: ev.gen });
      }
    }
    // 每轮失效：写升级步、世代、该轮发出的全部失效消息
    const writeUpgrades = [];
    for (let i = 1; i < steps.length; i++) {
      const ev = steps[i].event;
      if (ev && ev.type === 'write_upgrade') {
        const gen = steps[i].dir[ev.line].gen;
        const invIds = [...messages.values()]
          .filter((m) => m.kind === 'inv' && m.createdAt === i && m.line === ev.line)
          .map((m) => m.id)
          .sort((a, b) => a - b);
        writeUpgrades.push({ step: i, core: ev.core, line: ev.line, gen, invIds });
      }
    }
    // 某线某世代写入的闭合步与最后一击确认者
    function closureOf(line, gen) {
      for (let i = 1; i < steps.length; i++) {
        const pre = steps[i - 1].dir[line].pending;
        const post = steps[i].dir[line].pending;
        if (pre && pre.gen === gen && post === null) {
          const ev = steps[i].event;
          return { step: i, closedBy: ev && ev.type === 'ack' ? ev.core : null };
        }
      }
      return null;
    }
    return { steps, messages, deliveries, acks, writeUpgrades, closureOf };
  }

  // 沿 dupOf 回到原始消息编号
  function rootIdOf(index, msgId) {
    const seen = new Set();
    let cur = msgId;
    while (cur != null && !seen.has(cur)) {
      seen.add(cur);
      const m = index.messages.get(cur);
      if (!m || m.dupOf === null) return cur;
      cur = m.dupOf;
    }
    return cur;
  }

  // 同一条消息的完整重传家族：[原始, 副本1, 副本2, …]（按编号）
  function familyOf(index, msgId) {
    const root = rootIdOf(index, msgId);
    const ids = [root];
    for (const m of index.messages.values()) {
      if (m.id !== root && rootIdOf(index, m.id) === root) ids.push(m.id);
    }
    ids.sort((a, b) => a - b);
    return ids;
  }

  function ring(r) {
    return Object.assign({
      step: null, role: null, peer: false, historical: false, selected: false,
      msgId: null, line: null, core: null, gen: null,
      action: '', dupOf: null, replayOf: null, closes: false,
      alsoCreated: [], violation: null,
    }, r);
  }

  function vioAt(index, step) {
    const v = step ? index.steps[step].violation : null;
    return v ? { kind: v.kind, label: VIOLATION_LABELS[v.kind] || v.kind, detail: v.detail, step: v.step } : null;
  }

  // 一条失效/授权消息的全部重传环与投递环（按步序）
  function lineageRings(index, lineage, opts) {
    const out = [];
    // 超时重传环：除原始消息外，每个副本都有其 timeout 产生步
    for (const id of lineage.slice(1)) {
      const m = index.messages.get(id);
      const parent = index.messages.get(m.dupOf);
      out.push(ring({
        step: m.createdAt, role: 'retransmit', msgId: id,
        line: m.line, core: m.to, gen: m.gen,
        action: index.steps[m.createdAt].note,
        dupOf: parent ? parent.id : null,
      }));
    }
    // 投递环：整条重传线上的任意副本投递；最早一次为既有生效动作
    const delivered = [];
    for (const id of lineage) {
      for (const s of (index.deliveries.get(id) || [])) {
        if (!opts || !opts.before || s <= opts.before) delivered.push({ id, s });
      }
    }
    delivered.sort((a, b) => a.s - b.s);
    delivered.forEach((d, k) => {
      const m = index.messages.get(d.id);
      const root = index.messages.get(lineage[0]);
      out.push(ring({
        step: d.s,
        role: k === 0 ? 'deliver' : 'deliver-dup',
        msgId: d.id, line: m.line, core: m.to, gen: m.gen,
        action: index.steps[d.s].note,
        dupOf: m.dupOf,
        replayOf: k === 0 ? null : delivered[0].s,
        violation: vioAt(index, d.s),
      }));
    });
    return out;
  }

  function ackRingsFor(index, line, gen, core, afterStep, opts) {
    const out = [];
    for (const a of index.acks) {
      if (a.line !== line || a.gen !== gen || a.core !== core) continue;
      if (afterStep != null && a.step < afterStep) continue;
      if (opts && opts.before != null && a.step > opts.before) continue;
      const pre = index.steps[a.step - 1].dir[line].pending;
      const post = index.steps[a.step].dir[line].pending;
      out.push(ring({
        step: a.step, role: 'ack', msgId: null,
        line, core, gen,
        action: index.steps[a.step].note,
        closes: !!(pre && pre.gen === gen && post === null),
        violation: vioAt(index, a.step),
      }));
    }
    return out;
  }

  // 同世代旁证：同一写升级发出的其他失效消息及其投递、确认
  function peerRings(index, wu, selfRootId) {
    const out = [];
    for (const id of wu.invIds) {
      if (id === selfRootId) continue;
      const m = index.messages.get(id);
      const ln = familyOf(index, id);
      for (const r of lineageRings(index, ln)) {
        r.peer = true;
        out.push(r);
      }
      const dstep = ln.map((d) => index.deliveries.get(d) || [])
        .reduce((a, b) => a.concat(b), [])
        .filter((s) => !index.steps[s].violation)
        .sort((a, b) => a - b)[0];
      for (const r of ackRingsFor(index, wu.line, wu.gen, m.to, dstep != null ? dstep : null)) {
        r.peer = true;
        out.push(r);
      }
    }
    return out;
  }

  function finalizeRings(index, rings, selectedMsgId) {
    rings.sort((a, b) => {
      if (a.step !== b.step) return (a.step ?? 1e9) - (b.step ?? 1e9);
      return (a.msgId || 0) - (b.msgId || 0);
    });
    let end = rings.length;
    for (let i = 0; i < rings.length; i++) {
      if (rings[i].violation) { end = i + 1; break; } // 链路止于首个违规步
    }
    rings = rings.slice(0, end);
    if (selectedMsgId != null) {
      let target = rings.find((r) => r.msgId === selectedMsgId && (r.role === 'retransmit' || r.role === 'origin'));
      if (!target) target = rings.find((r) => r.msgId === selectedMsgId);
      if (target) target.selected = true;
    }
    return rings;
  }

  function buildMessageChain(index, msgId) {
    const lineage = familyOf(index, msgId);
    const rootId = lineage[0];
    const root = index.messages.get(rootId);
    if (!root) return { ok: false, error: `消息 M${msgId} 不存在于该记录` };
    const queried = index.messages.get(msgId);

    const createdEvent = index.steps[root.createdAt].event;
    const rings = [];
    let wu = null;

    // 第一环：产生该消息的原始请求
    if (createdEvent && createdEvent.type === 'write_upgrade') {
      wu = index.writeUpgrades.find((w) => w.step === root.createdAt) || null;
      rings.push(ring({
        step: root.createdAt, role: 'origin', msgId: rootId,
        line: root.line, core: createdEvent.core, gen: root.gen,
        action: index.steps[root.createdAt].note,
        alsoCreated: wu ? wu.invIds.filter((x) => x !== rootId) : [],
      }));
    } else {
      rings.push(ring({
        step: root.createdAt, role: 'origin', msgId: rootId,
        line: root.line,
        core: createdEvent ? createdEvent.core : null,
        gen: root.gen,
        action: index.steps[root.createdAt].note,
      }));
    }

    for (const r of lineageRings(index, lineage)) rings.push(r);

    // 失效消息：同世代确认（沿真正收到失效的目标核心）；授权消息无确认环
    if (root.kind === 'inv') {
      const deliveredSteps = lineage
        .map((id) => index.deliveries.get(id) || [])
        .reduce((a, b) => a.concat(b), [])
        .filter((s) => !index.steps[s].violation)
        .sort((a, b) => a - b);
      const first = deliveredSteps[0];
      if (first != null) {
        for (const r of ackRingsFor(index, root.line, root.gen, root.to, first)) rings.push(r);
      }
      if (wu) {
        for (const r of peerRings(index, wu, rootId)) rings.push(r);
      }
    }

    const closed = root.kind === 'inv' ? index.closureOf(root.line, root.gen) : null;
    const final = index.steps[index.steps.length - 1];
    const stillInFlight = final.inFlight.some((m) => lineage.includes(m.id));
    const terminator = rings.find((r) => r.violation);
    const ownAck = rings.find((r) => r.role === 'ack' && !r.peer && !r.historical && !r.violation);

    let endNote;
    if (terminator) {
      endNote = `链路终止于首个违约步 #${terminator.step}：${terminator.violation.label} — ${terminator.violation.detail}`;
    } else if (closed) {
      endNote = `第 ${root.gen} 世代写入已于 #${closed.step} 闭合（最后确认来自 C${closed.closedBy}）`;
    } else if (root.kind === 'inv' && stillInFlight) {
      endNote = '消息仍在途：记录中尚无其投递或同世代确认';
    } else if (root.kind === 'grant' && stillInFlight) {
      endNote = '授权（或其重传副本）仍在途：记录中尚无其投递';
    } else if (root.kind === 'inv' && !ownAck) {
      endNote = `失效已送达 C${root.to}，但记录中缺失其第 ${root.gen} 世代确认（等待关系未完成）`;
    } else if (root.kind === 'inv') {
      endNote = `C${root.to} 的第 ${root.gen} 世代确认已接受，但同世代等待集合尚未清空，写入未闭合`;
    } else {
      endNote = '授权已投递，授权链路无确认环节';
    }

    const finalized = finalizeRings(index, rings, msgId);
    return {
      ok: true,
      query: { kind: 'message', msgId },
      subject: {
        msgId, rootMsgId: rootId, kind: root.kind,
        line: root.line, core: root.to, gen: root.gen, dupOf: queried ? queried.dupOf : null,
      },
      rings: finalized,
      relation: null,
      terminatedByViolation: !!terminator && finalized.some((r) => r.violation),
      endNote,
    };
  }

  function historicalRound(index, line, gen, ackCore) {
    const wu = index.writeUpgrades.filter((w) => w.line === line && w.gen === gen).pop() || null;
    if (!wu) return null;
    const closure = index.closureOf(line, gen);
    const invId = wu.invIds.find((id) => index.messages.get(id).to === ackCore) || null;
    return {
      gen,
      originStep: wu.step,
      closedAt: closure ? closure.step : null,
      closedBy: closure ? closure.closedBy : null,
      targetedCore: invId != null,
      invId,
    };
  }

  function buildAckChain(index, ackStep) {
    const steps = index.steps;
    const ev = steps[ackStep].event;
    if (!ev || ev.type !== 'ack') return { ok: false, error: `第 ${ackStep} 步不是确认事件` };
    const line = ev.line;
    const pre = steps[ackStep - 1];
    const pending = pre.dir[line].pending;
    const v = vioAt(index, ackStep);
    const rings = [];
    let relation = null;

    // 确认环（接受或拒绝）
    const ackRing = ring({
      step: ackStep, role: 'ack', msgId: null,
      line, core: ev.core, gen: ev.gen,
      action: steps[ackStep].note,
      closes: !!(pending && pending.gen === ev.gen && steps[ackStep].dir[line].pending === null),
      violation: v,
    });

    if (!v) {
      // 合法确认：原始写升级 → 该核心的失效（含重传/投递）→ 确认
      const wu = index.writeUpgrades.filter((w) => w.line === line && w.gen === pending.gen).pop();
      const invId = wu.invIds.find((id) => index.messages.get(id).to === ev.core);
      rings.push(ring({
        step: wu.step, role: 'origin', msgId: invId || null,
        line, core: wu.core, gen: pending.gen,
        action: steps[wu.step].note,
        alsoCreated: wu.invIds.filter((x) => x !== invId),
      }));
      const ln = familyOf(index, invId);
      for (const r of lineageRings(index, ln, { before: ackStep })) rings.push(r);
      rings.push(ackRing);
      relation = {
        status: 'match',
        pendingGen: pending.gen, waitAcks: pending.waitAcks.slice(),
        requester: pending.requester, originStep: wu.step,
        explain: `确认属于第 ${pending.gen} 世代等待集合 {${pending.waitAcks.map((c) => 'C' + c).join(',')}}，等待关系匹配`,
      };
    } else {
      // 迟到/来源不明确认：先摆旧轮证据，再摆当前真实等待关系，最后以拒绝环终止
      const hist = historicalRound(index, line, ev.gen, ev.core);
      if (hist) {
        const hw = index.writeUpgrades.find((w) => w.step === hist.originStep);
        rings.push(ring({
          step: hw.step, role: 'origin', historical: true,
          msgId: hist.invId || (hw.invIds[0] || null),
          line, core: hw.core, gen: hist.gen,
          action: steps[hw.step].note,
          alsoCreated: hw.invIds.filter((x) => x !== (hist.invId || hw.invIds[0])),
        }));
        if (hist.closedAt != null) {
          rings.push(ring({
            step: hist.closedAt, role: 'ack', historical: true,
            line, core: hist.closedBy, gen: hist.gen, closes: true,
            action: steps[hist.closedAt].note,
          }));
        }
      }

      let status; let explain;
      if (!pending) {
        status = 'no-pending';
        explain = hist
          ? `第 ${ev.gen} 世代写入已于 #${hist.closedAt} 闭合，目录当前无等待中的写入：等待关系已不存在，旧确认必须拒绝`
          : `L${line} 从未发起第 ${ev.gen} 世代失效，目录也无等待中的写入：确认的等待关系缺失`;
      } else {
        const cur = index.writeUpgrades.filter((w) => w.line === line && w.gen === pending.gen).pop();
        // 当前真实等待轮的原始请求与各失效投递
        rings.push(ring({
          step: cur.step, role: 'origin', msgId: cur.invIds[0] || null,
          line, core: cur.core, gen: cur.gen,
          action: steps[cur.step].note,
          alsoCreated: cur.invIds.slice(1),
        }));
        const invs = cur.invIds.map((id) => {
          const m = index.messages.get(id);
          const ln = familyOf(index, id);
          const dstep = ln.map((d) => (index.deliveries.get(d) || [])[0])
            .filter((s) => s != null).sort((a, b) => a - b)[0] || null;
          return { msgId: id, to: m.to, gen: m.gen, deliveredStep: dstep };
        });
        for (const info of invs) {
          const ln = familyOf(index, info.msgId);
          for (const r of lineageRings(index, ln, { before: ackStep })) rings.push(r);
        }
        if (pending.gen !== ev.gen) {
          status = 'gen-mismatch';
          explain = `确认携带世代 ${ev.gen}，目录当前等待第 ${pending.gen} 世代（请求者 C${pending.requester}，等待集合 {${pending.waitAcks.map((c) => 'C' + c).join(',')}}）：世代失配，旧确认不得闭合新一轮写入`;
        } else if (!pending.waitAcks.includes(ev.core)) {
          status = 'not-waiting';
          explain = `C${ev.core} 不在第 ${pending.gen} 世代等待集合 {${pending.waitAcks.map((c) => 'C' + c).join(',')}} 中：该确认与本轮等待关系无关`;
        } else {
          status = 'no-owed-ack';
          explain = `C${ev.core} 未登记第 ${ev.gen} 世代的待确认失效（失效未投递）：确认缺少来源等待关系`;
        }
        relation = {
          status,
          pendingGen: pending.gen, waitAcks: pending.waitAcks.slice(),
          requester: pending.requester, originStep: cur.step, invs,
          historical: hist ? {
            gen: hist.gen, originStep: hist.originStep,
            closedAt: hist.closedAt, closedBy: hist.closedBy, targetedCore: hist.targetedCore,
          } : null,
          explain,
        };
      }
      if (!relation) {
        relation = {
          status,
          pendingGen: null, waitAcks: [], requester: null, originStep: null, invs: [],
          historical: hist ? {
            gen: hist.gen, originStep: hist.originStep,
            closedAt: hist.closedAt, closedBy: hist.closedBy, targetedCore: hist.targetedCore,
          } : null,
          explain,
        };
      }
      rings.push(ackRing);
    }

    const finalized = finalizeRings(index, rings, null);
    const sel = finalized.find((r) => r.step === ackStep && r.role === 'ack');
    if (sel) sel.selected = true;
    return {
      ok: true,
      query: { kind: 'ack', step: ackStep },
      subject: { kind: 'ack', msgId: null, line, core: ev.core, gen: ev.gen },
      rings: finalized,
      relation,
      terminatedByViolation: !!v,
      endNote: v
        ? `链路终止于首个违约步 #${ackStep}：${v.label} — ${v.detail}`
        : (ackRing.closes
          ? `第 ${ev.gen} 世代写入已于 #${ackStep} 闭合（最后确认来自 C${ev.core}）`
          : `确认接受，第 ${ev.gen} 世代仍等待其余核心`),
    };
  }

  function buildChain(result, query) {
    if (!result || !result.ok || !Array.isArray(result.steps)) {
      return { ok: false, error: '因果链需要一次完整的回放结果' };
    }
    if (!query || typeof query !== 'object') return { ok: false, error: '缺少链路查询' };
    const index = buildTraceIndex(result);
    if (query.kind === 'message') {
      if (!Number.isInteger(query.msgId) || query.msgId < 1) return { ok: false, error: '消息号非法' };
      return buildMessageChain(index, query.msgId);
    }
    if (query.kind === 'ack') {
      if (!Number.isInteger(query.step) || query.step < 1 || query.step > result.steps.length - 1) {
        return { ok: false, error: '确认事件步非法' };
      }
      return buildAckChain(index, query.step);
    }
    return { ok: false, error: `未知链路查询类型 ${query.kind}` };
  }

  // 页面选择入口：列出记录中全部可回溯对象（消息 + 确认）
  function listTraceables(result) {
    if (!result || !result.ok) return { messages: [], acks: [] };
    const index = buildTraceIndex(result);
    const finalStep = result.steps[result.steps.length - 1];
    const messages = [...index.messages.values()]
      .sort((a, b) => a.id - b.id)
      .map((m) => {
        const dSteps = index.deliveries.get(m.id) || [];
        const deliveredStep = dSteps[0] != null ? dSteps[0] : null;
        const frozenStep = dSteps.find((s) => result.steps[s].violation) || null;
        return {
          kind: 'message', msgId: m.id, msgKind: m.kind,
          to: m.to, line: m.line, gen: m.gen, dupOf: m.dupOf,
          createdAt: m.createdAt, deliveredStep, frozenStep,
          inFlightAtEnd: finalStep.inFlight.some((x) => x.id === m.id),
        };
      });
    const acks = index.acks.map((a) => ({
      kind: 'ack', step: a.step, core: a.core, line: a.line, gen: a.gen,
      rejected: !!result.steps[a.step].violation,
    }));
    return { messages, acks };
  }

  // ---------- 快照与主循环 ----------

  function snapshot(state, index, event, note, vio) {
    return {
      index,
      event: event ? clone(event) : null,
      note,
      violation: vio ? clone(vio) : null,
      dir: clone(state.dir),
      caches: clone(state.caches),
      inFlight: clone(state.inFlight),
    };
  }

  function simulate(input) {
    const inputError = validateInput(input);
    if (inputError) return { ok: false, error: inputError };

    const state = createState(input.cores, input.lines);
    const steps = [snapshot(state, 0, null, '初始状态：目录全 I，世代 0，无在途消息', null)];
    let frozeAt = null;

    for (let i = 0; i < input.events.length; i++) {
      const ev = input.events[i];
      const evErr = validateEvent(ev, input.cores, input.lines);
      let note; let vio = null;
      if (evErr) {
        vio = violation(VIOLATION.INVALID_EVENT, null, null, null, `事件 ${i + 1} 非法：${evErr}`);
        note = '事件校验失败';
      } else {
        const r = applyEvent(state, ev);
        note = r.note;
        vio = r.violation || checkConsistency(state);
      }
      if (vio) vio.step = i + 1;
      steps.push(snapshot(state, i + 1, ev, note, vio));
      if (vio) { frozeAt = i + 1; break; } // 首次违约步冻结
    }

    return {
      ok: true,
      steps,
      violation: frozeAt !== null ? steps[steps.length - 1].violation : null,
      eventsTotal: input.events.length,
      eventsProcessed: frozeAt !== null ? frozeAt : input.events.length,
    };
  }

  return {
    LIMITS, VIOLATION, VIOLATION_LABELS, CHAIN_ROLE_LABELS,
    createState, simulate, checkConsistency, effectiveHolders,
    buildChain, listTraceables,
  };
});
