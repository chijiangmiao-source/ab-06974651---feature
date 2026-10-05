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

  // ---------- 因果链追溯 ----------
  //
  // 审查员选中一条在途（或曾出现的）消息时，从逐步快照反推出有序因果链：
  //   原始请求（写升级/读缺失）→ 原始消息 → 超时重传副本 → 投递 → 同世代确认 → 闭合；
  // 重传副本通过 dupOf 关联其原始消息，重复投递环只指向既有投递动作，
  // 迟到/失配确认与过期消息在首个违约环终止并给出拒绝原因。
  // 追溯是对 simulate 结果的纯函数派生，本身不产生或修改任何证据。

  function mkLink(o) {
    return {
      role: o.role,            // origin / timeout / send / deliver / ack / close / pending / refuse
      step: o.step,            // 事件序号（1 起；pending 环取末步）
      order: o.order !== undefined ? o.order : 9,  // 同一步内的因果先后
      msgId: o.msgId !== undefined ? o.msgId : null,
      msgIds: o.msgIds || null,        // 一环涉及多条消息（如写升级同时发出若干失效）
      msgRefId: o.msgRefId !== undefined ? o.msgRefId : null, // 非消息环节引用的既有消息（如确认↔失效）
      line: o.line !== undefined ? o.line : null,
      core: o.core !== undefined ? o.core : null,
      gen: o.gen !== undefined ? o.gen : null,
      action: o.action,
      result: o.result || '',
      status: o.status || 'info',      // info / ok / replay / bad / pending
      violation: o.violation || null,
    };
  }

  // 从快照序列重建消息登记簿：每条曾出现消息的出生步、dupOf 溯源与投递/超时效应。
  function buildRegistry(result) {
    const byId = new Map();
    const effects = new Map(); // msgId -> [{type:'deliver'|'timeout', step, newId, violation}]
    const acks = [];
    const linkedViolations = []; // 非投递/超时事件中、引用了某条消息的违约（如缺失拥有者数据）
    let prevIds = new Set();

    const addEffect = (id, e) => {
      if (!effects.has(id)) effects.set(id, []);
      effects.get(id).push(e);
    };

    for (let i = 1; i < result.steps.length; i++) {
      const s = result.steps[i];
      const ev = s.event;
      for (const m of s.inFlight) {
        if (!byId.has(m.id)) {
          let rootId = m.id;
          if (m.dupOf !== null && byId.has(m.dupOf)) rootId = byId.get(m.dupOf).rootId;
          byId.set(m.id, {
            id: m.id, kind: m.kind, to: m.to, line: m.line, gen: m.gen,
            dupOf: m.dupOf, rootId, bornStep: i,
            grantState: m.grantState !== undefined ? m.grantState : null,
          });
        }
      }
      if (ev) {
        if (ev.type === 'deliver') {
          addEffect(ev.msg, { type: 'deliver', step: i, violation: s.violation ? clone(s.violation) : null });
        } else if (ev.type === 'timeout') {
          let newId = null;
          for (const m of s.inFlight) {
            if (!prevIds.has(m.id)) { newId = m.id; break; }
          }
          addEffect(ev.msg, { type: 'timeout', step: i, newId, violation: s.violation ? clone(s.violation) : null });
        } else if (ev.type === 'ack') {
          acks.push({ step: i, core: ev.core, line: ev.line, gen: ev.gen, violation: s.violation ? clone(s.violation) : null });
        }
      }
      if (s.violation && s.violation.msgId !== null && s.violation.msgId !== undefined
        && (!ev || (ev.type !== 'deliver' && ev.type !== 'timeout'))) {
        linkedViolations.push({ step: i, msgId: s.violation.msgId, violation: clone(s.violation) });
      }
      prevIds = new Set(s.inFlight.map((m) => m.id));
    }
    return { byId, effects, acks, linkedViolations };
  }

  function deliveriesOf(reg, id) {
    return (reg.effects.get(id) || [])
      .filter((e) => e.type === 'deliver')
      .sort((a, b) => a.step - b.step);
  }

  function copiesOf(reg, member) {
    return [...reg.byId.values()]
      .filter((r) => r.rootId === member.id)
      .sort((a, b) => (a.bornStep - b.bornStep) || (a.id - b.id));
  }

  // 一条消息（含其全部重传副本）的出生/超时/投递环。
  function messageTimelineLinks(result, reg, member) {
    const links = [];
    const copies = copiesOf(reg, member);
    for (const c of copies) {
      if (c.dupOf !== null) {
        links.push(mkLink({
          role: 'timeout', step: c.bornStep, order: 1, msgId: c.dupOf,
          line: member.line, core: member.to, gen: member.gen,
          action: `超时：重传在途消息 M${c.dupOf} → M${c.id}`,
          result: result.steps[c.bornStep].note, status: 'ok',
        }));
      }
      links.push(mkLink({
        role: 'send', step: c.bornStep, order: 2, msgId: c.id,
        line: member.line, core: member.to, gen: member.gen,
        action: c.dupOf !== null
          ? `生成重传副本 M${c.id}（dupOf=M${c.dupOf}，原始来源 M${member.id}）`
          : (member.kind === 'inv'
              ? `生成失效请求 M${c.id} → C${member.to}（L${member.line}，世代 ${member.gen}）`
              : `生成授权 M${c.id}（${c.grantState}）→ C${member.to}（L${member.line}，世代 ${member.gen}）`),
        result: c.dupOf !== null
          ? '仅复制在途消息：缓存线、目标核心、世代与原件一致，不产生新动作或新来源'
          : result.steps[c.bornStep].note,
        status: 'info',
      }));
    }

    // 所有副本的首次投递按到达先后排序：首个到达者执行真实动作，
    // 之后到达的（无论原件还是副本）都只能回放既有动作。
    const arrivals = [];
    for (const c of copies) {
      const d = deliveriesOf(reg, c.id)[0];
      if (d) arrivals.push({ copy: c, step: d.step, violation: d.violation });
    }
    arrivals.sort((a, b) => (a.step - b.step) || (a.copy.id - b.copy.id));
    const firstDelivery = arrivals.length
      ? { step: arrivals[0].step, msgId: arrivals[0].copy.id } : null;

    for (const a of arrivals) {
      const c = a.copy;
      const replay = a.step !== firstDelivery.step;
      const viaDup = c.dupOf !== null;
      let action;
      if (member.kind === 'inv') {
        action = replay
          ? `重复投递 M${c.id}：仅回放第 ${firstDelivery.step} 步 M${firstDelivery.msgId} 的既有动作`
            + `（重新登记 C${member.to} 的世代 ${member.gen} 待确认），不另造来源`
          : `投递失效 M${c.id} 至 C${member.to}`
            + (viaDup ? `（重传副本 M${c.id} 先于原件到达，执行原件 M${member.id} 的动作）` : '');
      } else {
        action = replay
          ? `重复投递授权 M${c.id}：仅回放第 ${firstDelivery.step} 步 M${firstDelivery.msgId} 的既有动作（保持既有副本），不另造来源`
          : `投递授权 M${c.id} 至 C${member.to}`
            + (viaDup ? `（重传副本 M${c.id} 先于原件 M${member.id} 到达）` : '');
      }
      links.push(mkLink({
        role: 'deliver', step: a.step, order: 3, msgId: c.id,
        line: member.line, core: member.to, gen: member.gen,
        action, result: result.steps[a.step].note,
        status: a.violation ? 'bad' : (replay ? 'replay' : 'ok'),
        violation: a.violation,
      }));
    }

    // 非法超时（引用已不在在途集合的消息，无副本出生）：显式拒绝环，不伪造成重传。
    const copyIds = new Set(copies.map((c) => c.id));
    for (const c of copies) {
      for (const e of (reg.effects.get(c.id) || [])) {
        if (e.type !== 'timeout') continue;
        const born = e.newId !== null && copyIds.has(e.newId);
        if (born) continue;
        links.push(mkLink({
          role: 'refuse', step: e.step, order: 1, msgId: c.id,
          line: member.line, core: member.to, gen: member.gen,
          action: `超时重传 M${c.id} 被拒绝：该消息不在在途集合（已投递或从未在途）`,
          result: result.steps[e.step].note, status: 'bad', violation: e.violation,
        }));
      }
    }
    return { links, copies, firstDelivery };
  }

  // 失效消息对应核心的确认环（含闭合判定与缺失/失配等待关系）。
  function invAckLinks(result, reg, member, firstDelivery) {
    const links = [];
    const candidates = reg.acks
      .filter((a) => a.core === member.to && a.line === member.line && a.step > member.bornStep)
      .sort((a, b) => a.step - b.step);

    let accepted = false;
    let closed = false;
    let closeStep = null;
    for (const a of candidates) {
      if (accepted) {
        // 闭合之后的同类确认只可能是违规的重复确认
        if (a.violation) {
          links.push(mkLink({
            role: 'ack', step: a.step, order: 4, msgRefId: member.id,
            line: member.line, core: member.to, gen: a.gen,
            action: `迟到/重复确认：C${member.to} 世代 ${a.gen}（第 ${member.bornStep} 步的 M${member.id} 一轮已处理过）`,
            result: `${result.steps[a.step].note}｜该确认不对应任何现存等待关系，链路在此终止`,
            status: 'bad', violation: a.violation,
          }));
          return { links, pending: false, closed, closeStep, accepted, rejected: true };
        }
        continue;
      }
      if (a.violation) {
        const matched = a.gen === member.gen;
        const deliverTxt = firstDelivery
          ? `失效 M${member.id} 已于第 ${firstDelivery.step} 步送达`
          : `失效 M${member.id} 尚未送达（仍在途）`;
        links.push(mkLink({
          role: 'ack', step: a.step, order: 4, msgRefId: member.id,
          line: member.line, core: member.to, gen: a.gen,
          action: matched
            ? `C${member.to} 的世代 ${a.gen} 确认被拒绝（对应失效 M${member.id}）`
            : `迟到确认：C${member.to} 持世代 ${a.gen}，等待关系要求世代 ${member.gen}`,
          result: `${result.steps[a.step].note}｜${deliverTxt}；等待关系缺失或失配，不得据此闭合写入`,
          status: 'bad', violation: a.violation,
        }));
        return { links, pending: false, closed: false, closeStep: null, accepted: false, rejected: true };
      }
      if (a.gen !== member.gen) continue; // 属于其他世代的合法确认，与本消息无关
      const before = result.steps[a.step - 1].dir[member.line].pending;
      const after = result.steps[a.step].dir[member.line].pending;
      links.push(mkLink({
        role: 'ack', step: a.step, order: 4, msgRefId: member.id,
        line: member.line, core: member.to, gen: a.gen,
        action: `C${member.to} 的世代 ${a.gen} 确认接受（对应失效 M${member.id}）`,
        result: result.steps[a.step].note, status: 'ok',
      }));
      accepted = true;
      if (!after && before && before.gen === member.gen) {
        closed = true; closeStep = a.step;
        links.push(mkLink({
          role: 'close', step: a.step, order: 5, msgRefId: member.id,
          line: member.line, core: before.requester, gen: member.gen,
          action: `等待集合清空，第 ${member.gen} 世代写入闭合`,
          result: `C${before.requester} 获得独占（M，L${member.line}，世代 ${member.gen}）`,
          status: 'ok',
        }));
      }
    }
    return { links, pending: !accepted, closed, closeStep, accepted, rejected: false };
  }

  function pendingInvLink(result, member, copies, delivered) {
    const lastStep = result.steps.length - 1;
    const fin = result.steps[lastStep];
    const fp = fin.dir[member.line].pending;
    const waiting = fp && fp.gen === member.gen && fp.waitAcks.includes(member.to);
    const lastCopy = copies[copies.length - 1];
    const text = !delivered
      ? `失效尚未送达 C${member.to}：最新副本 M${lastCopy.id} 仍在途`
      : `失效已送达 C${member.to}，但其世代 ${member.gen} 确认未出现`;
    return mkLink({
      role: 'pending', step: lastStep, order: 9, msgId: lastCopy.id,
      line: member.line, core: member.to, gen: member.gen,
      action: waiting
        ? `等待关系未结清：C${member.to} 的世代 ${member.gen} 确认缺失`
        : `未见 C${member.to} 的世代 ${member.gen} 确认（回放于第 ${lastStep} 步停止）`,
      result: `${text}；等待集合 ${fp && fp.gen === member.gen ? `{${cores(fp.waitAcks)}}` : '（已无该世代等待）'}`,
      status: 'pending',
    });
  }

  // 将 {msg} 或 {step}（投递/超时/确认事件）解析到一条既有消息；找不到时显式标注，不伪造来源。
  function resolveTarget(result, reg, ref) {
    if (!ref) return {};
    if (ref.msg !== null && ref.msg !== undefined) {
      return { rec: reg.byId.get(ref.msg) || null };
    }
    if (ref.step !== null && ref.step !== undefined) {
      const s = result.steps[ref.step];
      if (!s || !s.event) return {};
      const ev = s.event;
      if (ev.type === 'deliver' || ev.type === 'timeout') {
        return {
          rec: reg.byId.get(ev.msg) || null,
          missingMsg: reg.byId.has(ev.msg) ? null : ev.msg,
          refStep: ref.step,
        };
      }
      if (ev.type === 'ack') {
        const exact = [...reg.byId.values()]
          .filter((r) => r.kind === 'inv' && r.to === ev.core && r.line === ev.line
            && r.gen === ev.gen && r.bornStep < ref.step)
          .sort((a, b) => b.bornStep - a.bornStep)[0] || null;
        if (exact) return { rec: exact, refStep: ref.step };
        const pend = s.dir[ev.line].pending;
        if (pend) {
          const current = [...reg.byId.values()]
            .find((r) => r.kind === 'inv' && r.to === ev.core && r.line === ev.line && r.gen === pend.gen) || null;
          if (current) return { rec: current, refStep: ref.step };
        }
        return {
          rec: null, refStep: ref.step,
          originlessAck: { step: ref.step, core: ev.core, line: ev.line, gen: ev.gen, violation: s.violation },
        };
      }
    }
    return {};
  }

  function originlessChain(result, target) {
    if (target.originlessAck) {
      const a = target.originlessAck;
      const sNote = result.steps[a.step].note;
      const links = [mkLink({
        role: 'ack', step: a.step, order: 4,
        line: a.line, core: a.core, gen: a.gen,
        action: `C${a.core} 对 L${a.line} 的世代 ${a.gen} 确认到达`,
        result: `${sNote}｜无匹配的既有失效请求或等待关系（不伪造来源），链路在首个违规步终止`,
        status: 'bad', violation: a.violation,
      })];
      // 缺失/失配的等待关系：用违约步的目录实况说明
      let waiting = null;
      const pd = result.steps[a.step].dir[a.line];
      if (!pd.pending) {
        waiting = `L${a.line} 目录无等待中的写入：C${a.core} 的世代 ${a.gen} 确认没有可匹配的等待关系`;
      } else if (pd.pending.gen !== a.gen) {
        waiting = `等待关系要求世代 ${pd.pending.gen}（等待集合 {${cores(pd.pending.waitAcks)}}），到达的却是世代 ${a.gen}：世代失配`;
      } else if (!pd.pending.waitAcks.includes(a.core)) {
        waiting = `世代 ${pd.pending.gen} 匹配，但 C${a.core} 不在等待集合 {${cores(pd.pending.waitAcks)}} 中`;
      } else {
        waiting = `C${a.core} 未登记世代 ${a.gen} 的待确认失效：等待关系存在但确认来源缺失/失配`;
      }
      return {
        ok: true, status: 'rejected', selected: null, rootId: null,
        links, terminal: links[0], terminalStep: a.step,
        rejection: a.violation ? { kind: a.violation.kind, detail: a.violation.detail } : null,
        notices: ['该确认找不到任何既有失效来源：等待关系缺失或失配'],
        waiting, closure: null,
      };
    }
    const msgId = target.missingMsg;
    const step = target.refStep;
    const links = [mkLink({
      role: 'refuse', step, order: 6, msgId,
      action: `引用消息 M${msgId}，但该消息从未生成或已离开在途集合`,
      result: `${result.steps[step].note}｜无可追溯的既有消息来源（不伪造来源）`,
      status: 'bad', violation: result.steps[step].violation,
    })];
    return {
      ok: true, status: 'rejected', selected: { id: msgId, missing: true }, rootId: null,
      links, terminal: links[0], terminalStep: step,
      rejection: result.steps[step].violation
        ? { kind: result.steps[step].violation.kind, detail: result.steps[step].violation.detail }
        : null,
      notices: [], waiting: null, closure: null,
    };
  }

  function traceCausalChain(result, ref) {
    if (!result || result.ok !== true) {
      return { ok: false, error: '缺少可追溯的回放结果', status: 'invalid', links: [] };
    }
    const reg = buildRegistry(result);
    const target = resolveTarget(result, reg, ref);
    if (!target.rec) return Object.assign({ ok: true }, originlessChain(result, target));

    const selected = target.rec;
    const root = reg.byId.get(selected.rootId);
    const idSet = new Set();
    const links = [];
    const notices = [];
    let closure = null;

    if (selected.dupOf !== null) {
      notices.push(`所选 M${selected.id} 是超时重传副本：dupOf=M${selected.dupOf}，原始来源 M${root.id}；重传不产生新动作或新来源`);
    }

    if (root.kind === 'inv') {
      // 同一次写升级、同一缓存线/世代发出的全部失效为兄弟消息，同世代失效与确认一并可追溯。
      const members = [...reg.byId.values()]
        .filter((r) => r.kind === 'inv' && r.line === root.line && r.gen === root.gen && r.bornStep === root.bornStep)
        .sort((a, b) => a.id - b.id);
      const originEv = result.steps[root.bornStep].event;
      links.push(mkLink({
        role: 'origin', step: root.bornStep, order: 0, msgIds: members.map((m) => m.id),
        line: root.line, core: originEv.core, gen: root.gen,
        action: `写升级 C${originEv.core} 发起第 ${root.gen} 世代失效（L${root.line}）`,
        result: result.steps[root.bornStep].note, status: 'info',
      }));
      const pendingLinks = [];
      for (const m of members) {
        idSet.add(m.id);
        const tl = messageTimelineLinks(result, reg, m);
        for (const c of tl.copies) idSet.add(c.id);
        links.push(...tl.links);
        const al = invAckLinks(result, reg, m, tl.firstDelivery);
        links.push(...al.links);
        if (al.closed && !closure) {
          closure = { step: al.closeStep, gen: m.gen, requester: result.steps[al.closeStep - 1].dir[m.line].pending.requester };
        }
        if (al.pending) {
          pendingLinks.push(pendingInvLink(result, m, tl.copies, !!tl.firstDelivery));
        }
      }
      links.push(...pendingLinks);
    } else {
      const originEv = result.steps[root.bornStep].event;
      links.push(mkLink({
        role: 'origin', step: root.bornStep, order: 0, msgIds: [root.id],
        line: root.line, core: originEv.core, gen: root.gen,
        action: `读缺失 C${originEv.core}（L${root.line}）请求数据`,
        result: result.steps[root.bornStep].note, status: 'info',
      }));
      const tl = messageTimelineLinks(result, reg, root);
      idSet.add(root.id);
      for (const c of tl.copies) idSet.add(c.id);
      links.push(...tl.links);
      if (!tl.firstDelivery) {
        const lastStep = result.steps.length - 1;
        const lastCopy = tl.copies[tl.copies.length - 1];
        links.push(mkLink({
          role: 'pending', step: lastStep, order: 9, msgId: lastCopy.id,
          line: root.line, core: root.to, gen: root.gen,
          action: `授权 M${lastCopy.id} 仍在途，尚未送达 C${root.to}`,
          result: `L${root.line} 的授权动作未完成（回放于第 ${lastStep} 步停止）`,
          status: 'pending',
        }));
      } else if (tl.links.some((l) => l.role === 'deliver' && !l.violation)) {
        closure = { step: tl.firstDelivery.step, gen: root.gen, kind: 'grant', to: root.to };
      }
    }

    // 其他事件引用链上消息的违约（如授权在途时的缺失拥有者数据）。
    for (const lv of reg.linkedViolations) {
      if (idSet.has(lv.msgId)) {
        links.push(mkLink({
          role: 'refuse', step: lv.step, order: 6, msgId: lv.msgId,
          line: lv.violation.line, core: Array.isArray(lv.violation.core) ? null : lv.violation.core,
          gen: null,
          action: `第 ${lv.step} 步的动作引用 M${lv.msgId} 时违约`,
          result: lv.violation.detail, status: 'bad', violation: lv.violation,
        }));
      }
    }

    links.sort((a, b) => (a.step - b.step) || (a.order - b.order)
      || ((a.msgId || 0) - (b.msgId || 0)) || ((a.msgRefId || 0) - (b.msgRefId || 0))
      || ((a.msgIds && a.msgIds[0]) || 0) - ((b.msgIds && b.msgIds[0]) || 0));

    let status = 'pending';
    let terminal = null;
    let rejection = null;
    const cut = links.findIndex((l) => l.status === 'bad');
    if (cut >= 0) {
      terminal = links[cut];
      links.splice(cut + 1);
      status = 'rejected';
      rejection = terminal.violation
        ? { kind: terminal.violation.kind, detail: terminal.violation.detail }
        : { kind: null, detail: terminal.result };
    } else if (closure) {
      status = 'closed';
    }

    // 迟到/失配确认：显式给出其缺失或失配的等待关系。
    let waiting = (() => {
      const miss = links.filter((l) => l.role === 'pending');
      return miss.length ? miss.map((l) => l.action).join('；') : null;
    })();
    if (status === 'rejected' && terminal.role === 'ack') {
      const pd = result.steps[terminal.step].dir[terminal.line];
      if (!pd.pending) {
        waiting = `L${terminal.line} 目录无等待中的写入：C${terminal.core} 的世代 ${terminal.gen} 确认没有可匹配的等待关系`;
      } else if (pd.pending.gen !== terminal.gen) {
        waiting = `等待关系要求世代 ${pd.pending.gen}（等待集合 {${cores(pd.pending.waitAcks)}}），到达的却是世代 ${terminal.gen}：世代失配`;
      } else if (!pd.pending.waitAcks.includes(terminal.core)) {
        waiting = `世代 ${pd.pending.gen} 匹配，但 C${terminal.core} 不在等待集合 {${cores(pd.pending.waitAcks)}} 中`;
      } else {
        waiting = `C${terminal.core} 未登记世代 ${terminal.gen} 的待确认失效：等待关系存在但确认来源缺失/失配`;
      }
    }

    return {
      ok: true,
      status,
      selected: {
        id: selected.id, kind: selected.kind, to: selected.to, line: selected.line,
        gen: selected.gen, dupOf: selected.dupOf,
      },
      rootId: root.id,
      links,
      terminal,
      terminalStep: terminal ? terminal.step : null,
      rejection,
      closure,
      waiting,
      notices,
    };
  }

  // 所有曾出生的消息（含已投递/被拒/重传副本），供页面选择入口使用。
  // state 与追溯结论保持一致：rejected=其因果链在首个违规步终止；
  // delivered=链已闭合；in-flight=仍有未完成的投递/等待。
  function listTraceableMessages(result) {
    if (!result || result.ok !== true) return [];
    const reg = buildRegistry(result);
    const finalIds = new Set(result.steps[result.steps.length - 1].inFlight.map((m) => m.id));
    return [...reg.byId.values()]
      .sort((a, b) => a.id - b.id)
      .map((r) => {
        let state;
        let stateStep = null;
        const own = (reg.effects.get(r.id) || []).find((e) => e.violation);
        if (own) {
          state = 'rejected';
          stateStep = own.step;
        } else {
          const ch = traceCausalChain(result, { msg: r.id });
          if (ch.status === 'rejected') { state = 'rejected'; stateStep = ch.terminalStep; }
          else if (ch.status === 'closed') { state = 'delivered'; stateStep = ch.closure.step; }
          else state = 'in-flight';
        }
        return {
          id: r.id, kind: r.kind, to: r.to, line: r.line, gen: r.gen,
          dupOf: r.dupOf, rootId: r.rootId, bornStep: r.bornStep,
          state, stateStep, inFlight: finalIds.has(r.id),
        };
      });
  }

  return {
    LIMITS, VIOLATION, VIOLATION_LABELS,
    createState, simulate, checkConsistency, effectiveHolders,
    traceCausalChain, listTraceableMessages,
  };
});
