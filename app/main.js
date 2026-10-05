/*
 * 主线程：导入/草稿/回放控制/渲染。
 * 计算全部委托给 Web Worker；runToken 保证用户取消或重新导入后，
 * 旧 Worker 的结果不会覆盖新内容。
 */
(function () {
  'use strict';

  var DRAFT_KEY = 'dirreplay.draft.v1';
  var PLAY_INTERVAL_MS = 650;

  var $ = function (s) { return document.querySelector(s); };

  var worker = null;
  var runToken = 0;
  var steps = null;
  var eventsTotal = 0;
  var idx = 0;
  var timer = null;
  var currentInput = null;   // 当前导入：因果链查询只能基于当前证据
  var traceables = null;     // 当前记录的全部可回溯对象
  var chainReqId = 0;        // 链路查询序号：旧查询结果不得覆盖新查询

  // ---------- 示例轨迹 ----------

  var SAMPLES = {
    legal: {
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
    },
    lateAck: {
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
    },
    missingData: {
      cores: 2,
      lines: 1,
      events: [
        { type: 'read_miss', core: 0, line: 0 },
        { type: 'read_miss', core: 1, line: 0 },
      ],
    },
  };

  // ---------- 状态栏 ----------

  function setStatus(t) { $('#status').textContent = t; }

  function setStepInfo() {
    if (!steps) { $('#stepInfo').textContent = ''; return; }
    $('#stepInfo').textContent = '第 ' + idx + ' / ' + (steps.length - 1) + ' 步（事件 ' +
      Math.min(idx, eventsTotal) + ' / ' + eventsTotal + '）';
  }

  // ---------- Worker 管理 ----------

  function stopPlay() {
    if (timer) { clearInterval(timer); timer = null; }
  }

  // 取消当前运行：递增令牌使任何在途结果失效，并终止 Worker
  function cancelRun(reason) {
    runToken++;
    if (worker) { worker.terminate(); worker = null; }
    stopPlay();
    if (reason) setStatus(reason);
  }

  function makeWorker(token) {
    var w = new Worker('worker.js');
    w.onmessage = function (e) {
      var msg = e.data;
      if (!msg || msg.token !== token) return; // 旧 Worker 结果一律丢弃
      if (msg.type === 'error') {
        if (msg.op === 'chain') {
          if (msg.reqId !== chainReqId) return;
          renderChain({ ok: false, error: '计算失败：' + msg.error });
        } else {
          setStatus('计算失败：' + msg.error);
        }
        return;
      }
      if (msg.op === 'chain') {
        if (msg.reqId !== chainReqId) return; // 旧链路查询不得覆盖当前链路
        renderChain(msg.chain);
        return;
      }
      var result = msg.result;
      if (!result.ok) {
        traceables = null;
        populateTraceSelectors({ messages: [], acks: [] });
        clearChain();
        setStatus('导入被拒绝：' + result.error);
        return;
      }
      steps = result.steps;
      eventsTotal = result.eventsTotal;
      traceables = msg.traceables;
      idx = 0;
      populateTraceSelectors(traceables);
      clearChain();
      renderEventList(result);
      renderStep();
      if (result.violation) {
        setStatus('回放冻结于第 ' + result.violation.step + ' 步（首次违约）');
      } else {
        setStatus('回放就绪：' + result.eventsProcessed + ' 个事件全部闭合，无违约');
      }
    };
    w.onerror = function (err) {
      if (token !== runToken) return;
      setStatus('Worker 错误：' + (err.message || err));
    };
    return w;
  }

  function runSimulation(input) {
    cancelRun();
    currentInput = input;
    chainReqId = 0;
    var token = runToken;
    setStatus('计算中…');
    steps = null;
    traceables = null;
    worker = makeWorker(token);
    worker.postMessage({ token: token, op: 'run', input: input });
  }

  // 因果链查询：复用当前 Worker，对当前导入重放取证；
  // 取消/重新导入会终止 Worker 并换令牌，旧查询不可能覆盖当前证据。
  function requestChain(query) {
    if (!worker || !currentInput) {
      renderChain({ ok: false, error: '当前没有可回溯的回放记录' });
      return;
    }
    chainReqId++;
    setChainStatus('因果链计算中…');
    worker.postMessage({
      token: runToken, op: 'chain', reqId: chainReqId,
      input: currentInput, query: query,
    });
  }

  // ---------- 导入与草稿 ----------

  function importText(text) {
    var input;
    try {
      input = JSON.parse(text);
    } catch (err) {
      setStatus('JSON 解析失败：' + err.message);
      return;
    }
    try {
      localStorage.setItem(DRAFT_KEY, text); // 本地草稿：重开页面可复查同一份逐步证据
    } catch (e) { /* 存储不可用时仅跳过持久化 */ }
    runSimulation(input);
  }

  function loadSample(name) {
    var text = JSON.stringify(SAMPLES[name], null, 2);
    $('#jsonInput').value = text;
    importText(text);
  }

  // ---------- 事件序列 ----------

  function fmtEvent(ev) {
    switch (ev.type) {
      case 'read_miss': return '读缺失 C' + ev.core + ' L' + ev.line;
      case 'write_upgrade': return '写升级 C' + ev.core + ' L' + ev.line +
        (ev.data !== undefined ? ' 数据=' + ev.data : '');
      case 'deliver': return '投递 M' + ev.msg;
      case 'timeout': return '超时重传 M' + ev.msg;
      case 'ack': return '确认 C' + ev.core + ' L' + ev.line + ' 世代' + ev.gen;
      default: return JSON.stringify(ev);
    }
  }

  function renderEventList(result) {
    var ol = $('#eventList');
    ol.innerHTML = '';
    result.steps.forEach(function (s, i) {
      if (i === 0) return; // 初始状态不占事件位
      var li = document.createElement('li');
      li.textContent = fmtEvent(s.event);
      li.dataset.step = String(i);
      if (s.violation) li.classList.add('bad');
      li.addEventListener('click', function () { stopPlay(); idx = i; renderStep(); });
      ol.appendChild(li);
    });
  }

  // ---------- 渲染 ----------

  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
    });
  }

  function renderStep() {
    if (!steps) return;
    var s = steps[idx];
    setStepInfo();

    // 事件高亮
    var items = $('#eventList').children;
    for (var i = 0; i < items.length; i++) {
      items[i].classList.toggle('active', Number(items[i].dataset.step) === idx);
    }

    // 说明
    $('#note').textContent = s.note || '';

    // 违约横幅
    var box = $('#violation');
    if (s.violation) {
      var v = s.violation;
      var labels = (typeof Protocol !== 'undefined') ? Protocol.VIOLATION_LABELS : {};
      var label = labels[v.kind] || v.kind;
      box.innerHTML = '⛔ 首次违约冻结 — <b>' + esc(label) + '</b>'
        + (v.line !== null && v.line !== undefined ? '｜线 L' + esc(v.line) : '')
        + (v.core !== null && v.core !== undefined
          ? '｜核心 ' + (Array.isArray(v.core) ? v.core.map(function (c) { return 'C' + c; }).join(',') : 'C' + esc(v.core))
          : '')
        + (v.msgId !== null && v.msgId !== undefined ? '｜消息 M' + esc(v.msgId) : '')
        + '<br>' + esc(v.detail);
      box.classList.remove('hidden');
    } else {
      box.classList.add('hidden');
      box.innerHTML = '';
    }

    renderDir(s);
    renderCaches(s);
    renderInflight(s);
  }

  function renderDir(s) {
    var html = '<thead><tr><th>线</th><th>状态</th><th>拥有者</th><th>共享者</th>' +
      '<th>数据</th><th>世代</th><th>等待确认</th></tr></thead><tbody>';
    s.dir.forEach(function (d) {
      var wait = d.pending
        ? 'C' + d.pending.requester + ' @世代' + d.pending.gen + ' 等 {' +
          d.pending.waitAcks.map(function (c) { return 'C' + c; }).join(',') + '}'
        : '—';
      html += '<tr class="st-' + d.state + '"><td>L' + d.line + '</td><td>' + d.state + '</td><td>' +
        (d.owner === null ? '—' : 'C' + d.owner) + '</td><td>{' +
        d.sharers.map(function (c) { return 'C' + c; }).join(',') + '}</td><td>' + d.data +
        '</td><td class="gen">g' + d.gen + '</td><td>' + wait + '</td></tr>';
    });
    $('#dirTable').innerHTML = html + '</tbody>';
  }

  function renderCaches(s) {
    var nLines = s.dir.length;
    var html = '<thead><tr><th>核\\线</th>';
    for (var l = 0; l < nLines; l++) html += '<th>L' + l + '</th>';
    html += '</tr></thead><tbody>';
    s.caches.forEach(function (c) {
      html += '<tr><th>C' + c.core + '</th>';
      c.lines.forEach(function (cl) {
        var sub = 'g' + cl.lastInvGen + (cl.owedAckGen !== null ? ' ⏳待确认g' + cl.owedAckGen : '');
        html += '<td class="st-' + cl.state + '"><b>' + cl.state + '</b>' +
          (cl.data !== null ? ' d=' + cl.data : '') + '<br><small>' + sub + '</small></td>';
      });
      html += '</tr>';
    });
    $('#cacheTable').innerHTML = html + '</tbody>';
  }

  function renderInflight(s) {
    var box = $('#inflight');
    if (s.inFlight.length === 0) { box.innerHTML = '<span class="dim">（无在途消息；可在下方“因果链回溯”选择已投递消息或确认）</span>'; return; }
    box.innerHTML = s.inFlight.map(function (m) {
      var body = m.kind === 'inv'
        ? '失效→C' + m.to + ' L' + m.line + ' g' + m.gen
        : '授权→C' + m.to + ' L' + m.line + ' ' + m.grantState + ' g' + m.gen + ' d=' + m.data;
      var dup = m.dupOf !== null ? ' <em>重传自 M' + m.dupOf + '</em>' : '';
      return '<button type="button" class="chip ' + m.kind + ' chainpick" data-msg="' + m.id + '" ' +
        'title="查看 M' + m.id + ' 的因果链">' +
        'M' + m.id + ' ' + esc(body) + dup + ' ⤳</button>';
    }).join(' ');
    box.querySelectorAll('.chainpick').forEach(function (b) {
      b.addEventListener('click', function () {
        $('#traceMsg').value = String(b.dataset.msg);
        requestChain({ kind: 'message', msgId: Number(b.dataset.msg) });
      });
    });
  }

  // ---------- 因果链回溯 ----------

  function msgOptionLabel(m) {
    var kind = m.msgKind === 'inv' ? '失效' : '授权';
    var s = 'M' + m.msgId + ' ' + kind + '→C' + m.to + ' L' + m.line + ' g' + m.gen;
    if (m.dupOf !== null) s += '（重传自 M' + m.dupOf + '）';
    if (m.frozenStep != null) s += '（# ' + m.frozenStep + ' 冻结）';
    else if (m.deliveredStep != null) s += '（# ' + m.deliveredStep + ' 已投递）';
    else if (m.inFlightAtEnd) s += '（仍在途）';
    else s += '（未投递即结束）';
    return s;
  }

  function populateTraceSelectors(t) {
    var ms = $('#traceMsg');
    var as = $('#traceAck');
    ms.innerHTML = '<option value="">— 选择消息 —</option>';
    as.innerHTML = '<option value="">— 选择确认 —</option>';
    t.messages.forEach(function (m) {
      var o = document.createElement('option');
      o.value = String(m.msgId);
      o.textContent = msgOptionLabel(m);
      ms.appendChild(o);
    });
    t.acks.forEach(function (a) {
      var o = document.createElement('option');
      o.value = String(a.step);
      o.textContent = '#' + a.step + ' 确认 C' + a.core + ' L' + a.line + ' g' + a.gen +
        (a.rejected ? '（被拒绝/冻结）' : '（接受）');
      as.appendChild(o);
    });
  }

  function setChainStatus(t) { $('#chainStatus').textContent = t || ''; }

  function clearChain() {
    $('#traceMsg').value = '';
    $('#traceAck').value = '';
    $('#chainView').innerHTML = '<p class="dim">点击在途消息标签，或从上方选择一条消息 / 确认，查看从原始请求出发的有序因果链。</p>';
    setChainStatus('');
  }

  function jumpToStep(step) {
    if (!steps || step == null || step < 0 || step >= steps.length) return;
    stopPlay();
    idx = step;
    renderStep();
  }

  function renderChain(c) {
    var box = $('#chainView');
    if (!c || !c.ok) {
      box.innerHTML = '<p class="chain-error">无法构建因果链：' + esc((c && c.error) || '未知错误') + '</p>';
      setChainStatus('');
      return;
    }
    var labels = (typeof Protocol !== 'undefined' && Protocol.CHAIN_ROLE_LABELS) ? Protocol.CHAIN_ROLE_LABELS : {};
    var subj = c.subject;
    var head = c.query.kind === 'message'
      ? '消息 M' + subj.msgId + (subj.dupOf !== null ? '（M' + subj.dupOf + ' 的重传副本，原始消息 M' + subj.rootMsgId + '）'
          : '（原始消息 M' + subj.rootMsgId + '）') +
        '｜' + (subj.kind === 'inv' ? '失效' : '授权') + '→C' + subj.core + ' L' + subj.line + ' g' + subj.gen
      : '#' + c.query.step + ' 确认 C' + subj.core + ' L' + subj.line + ' g' + subj.gen;

    var html = '<div class="chain-head' + (c.terminatedByViolation ? ' bad' : '') + '">⤳ ' + esc(head) + '</div>';

    // 等待关系（迟到确认时为缺失/失配证据）
    if (c.relation) {
      var r = c.relation;
      html += '<div class="chain-relation ' + r.status + '"><b>等待关系：</b>' + esc(r.explain);
      if (r.invs && r.invs.length) {
        html += '<br><small>当前等待轮的失效投递：' + r.invs.map(function (x) {
          return 'M' + x.msgId + '→C' + x.to +
            (x.deliveredStep != null ? '（# ' + x.deliveredStep + ' 送达）' : '（尚未送达）');
        }).join('；') + '</small>';
      }
      if (r.historical && r.historical.closedAt != null) {
        html += '<br><small>旧第 ' + r.historical.gen + ' 世代已于 # ' + r.historical.closedAt +
          ' 闭合（最后确认 C' + r.historical.closedBy + '），其等待集合已不存在。</small>';
      }
      html += '</div>';
    }

    html += '<ol class="chain">';
    c.rings.forEach(function (rn) {
      var role = labels[rn.role] || rn.role;
      var cls = ['chain-ring', 'role-' + rn.role];
      if (rn.peer) cls.push('peer');
      if (rn.historical) cls.push('historical');
      if (rn.selected) cls.push('selected');
      if (rn.violation) cls.push('viol');
      var tags = [];
      if (rn.peer) tags.push('同世代旁证');
      if (rn.historical) tags.push('旧轮证据');
      if (rn.selected) tags.push('所选对象');
      if (rn.dupOf !== null) tags.push('dupOf=M' + rn.dupOf);
      if (rn.replayOf != null) tags.push('仅回放 # ' + rn.replayOf + ' 的既有动作，无新来源');
      if (rn.closes) tags.push('写入闭合');
      html += '<li class="' + cls.join(' ') + '" data-step="' + (rn.step == null ? '' : rn.step) + '">'
        + '<div class="ring-head">'
        + '<span class="ring-step">#' + (rn.step == null ? '—' : rn.step) + '</span>'
        + '<span class="ring-role">' + esc(role) + '</span>'
        + (tags.length ? '<span class="ring-tags">' + tags.map(esc).join('｜') + '</span>' : '')
        + '</div>'
        + '<div class="ring-fields">'
        + '<span>消息 ' + (rn.msgId == null ? '—' : 'M' + rn.msgId) + '</span>'
        + '<span>线 ' + (rn.line == null ? '—' : 'L' + rn.line) + '</span>'
        + '<span>核心 ' + (rn.core == null ? '—' : 'C' + rn.core) + '</span>'
        + '<span>世代 ' + (rn.gen == null ? '—' : 'g' + rn.gen) + '</span>'
        + '</div>'
        + '<div class="ring-action">' + esc(rn.action || '—') + '</div>';
      if (rn.alsoCreated && rn.alsoCreated.length) {
        html += '<div class="ring-extra">同一步还产生：' + rn.alsoCreated.map(function (id) { return 'M' + id; }).join('、') + '</div>';
      }
      if (rn.violation) {
        html += '<div class="ring-reject">⛔ 拒绝原因（' + esc(rn.violation.label) + '）：' + esc(rn.violation.detail) + '</div>';
      }
      html += '</li>';
    });
    html += '</ol>';
    html += '<div class="chain-end' + (c.terminatedByViolation ? ' bad' : '') + '">' + esc(c.endNote) + '</div>';
    box.innerHTML = html;
    setChainStatus(c.terminatedByViolation ? '链路终止于首个违规步骤' : '因果链完整');
    box.querySelectorAll('.chain-ring').forEach(function (li) {
      li.addEventListener('click', function () { jumpToStep(Number(li.dataset.step)); });
    });
  }

  // ---------- 回放控制 ----------

  function stepTo(i) {
    if (!steps) return;
    idx = Math.max(0, Math.min(steps.length - 1, i));
    renderStep();
  }

  function bind() {
    $('#btnImport').addEventListener('click', function () { importText($('#jsonInput').value); });
    document.querySelectorAll('[data-sample]').forEach(function (b) {
      b.addEventListener('click', function () { loadSample(b.dataset.sample); });
    });
    $('#btnClearDraft').addEventListener('click', function () {
      try { localStorage.removeItem(DRAFT_KEY); } catch (e) { /* ignore */ }
      $('#jsonInput').value = '';
      cancelRun('本地草稿已清除');
      steps = null; currentInput = null; traceables = null;
      $('#eventList').innerHTML = ''; renderEmptyTables(); setStepInfo();
    });
    $('#btnPrev').addEventListener('click', function () { stopPlay(); stepTo(idx - 1); });
    $('#btnNext').addEventListener('click', function () { stopPlay(); stepTo(idx + 1); });
    $('#btnReset').addEventListener('click', function () { stopPlay(); stepTo(0); });
    $('#btnPlay').addEventListener('click', function () {
      if (!steps || timer) return;
      timer = setInterval(function () {
        if (idx >= steps.length - 1) { stopPlay(); return; }
        idx++; renderStep();
      }, PLAY_INTERVAL_MS);
    });
    $('#btnPause').addEventListener('click', stopPlay);
    $('#btnCancel').addEventListener('click', function () {
      cancelRun('已取消：旧结果不会覆盖当前内容');
      steps = null; currentInput = null; traceables = null;
      $('#eventList').innerHTML = ''; renderEmptyTables(); setStepInfo();
    });

    $('#traceMsg').addEventListener('change', function () {
      if (this.value === '') return;
      $('#traceAck').value = '';
      requestChain({ kind: 'message', msgId: Number(this.value) });
    });
    $('#traceAck').addEventListener('change', function () {
      if (this.value === '') return;
      $('#traceMsg').value = '';
      requestChain({ kind: 'ack', step: Number(this.value) });
    });
  }

  function renderEmptyTables() {
    $('#dirTable').innerHTML = '';
    $('#cacheTable').innerHTML = '';
    $('#inflight').innerHTML = '';
    $('#note').textContent = '';
    $('#violation').classList.add('hidden');
    $('#traceMsg').innerHTML = '';
    $('#traceAck').innerHTML = '';
    $('#chainView').innerHTML = '';
    setChainStatus('');
  }

  // ---------- 启动 ----------

  bind();
  var draft = null;
  try { draft = localStorage.getItem(DRAFT_KEY); } catch (e) { /* ignore */ }
  if (draft) {
    $('#jsonInput').value = draft;
    importText(draft); // 重新打开本地草稿：确定性重放同一份逐步证据
  } else {
    $('#jsonInput').value = JSON.stringify(SAMPLES.legal, null, 2);
    importText($('#jsonInput').value);
  }
})();
