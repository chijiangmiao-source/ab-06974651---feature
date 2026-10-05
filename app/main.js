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

  function runSimulation(input) {
    cancelRun();
    var token = runToken;
    setStatus('计算中…');
    steps = null;
    worker = new Worker('worker.js');
    worker.onmessage = function (e) {
      var msg = e.data;
      if (!msg || msg.token !== token) return; // 旧结果不得覆盖新导入内容
      if (msg.type === 'error') { setStatus('计算失败：' + msg.error); return; }
      var result = msg.result;
      if (!result.ok) { setStatus('导入被拒绝：' + result.error); return; }
      steps = result.steps;
      eventsTotal = result.eventsTotal;
      idx = 0;
      renderEventList(result);
      renderStep();
      if (result.violation) {
        setStatus('回放冻结于第 ' + result.violation.step + ' 步（首次违约）');
      } else {
        setStatus('回放就绪：' + result.eventsProcessed + ' 个事件全部闭合，无违约');
      }
    };
    worker.onerror = function (err) {
      if (token !== runToken) return;
      setStatus('Worker 错误：' + (err.message || err));
    };
    worker.postMessage({ token: token, input: input });
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
    if (s.inFlight.length === 0) { box.innerHTML = '<span class="dim">（无在途消息）</span>'; return; }
    box.innerHTML = s.inFlight.map(function (m) {
      var body = m.kind === 'inv'
        ? '失效→C' + m.to + ' L' + m.line + ' g' + m.gen
        : '授权→C' + m.to + ' L' + m.line + ' ' + m.grantState + ' g' + m.gen + ' d=' + m.data;
      var dup = m.dupOf !== null ? ' <em>重传自 M' + m.dupOf + '</em>' : '';
      return '<span class="chip ' + m.kind + '">M' + m.id + ' ' + esc(body) + dup + '</span>';
    }).join(' ');
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
      steps = null; $('#eventList').innerHTML = ''; renderEmptyTables(); setStepInfo();
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
      steps = null; $('#eventList').innerHTML = ''; renderEmptyTables(); setStepInfo();
    });
  }

  function renderEmptyTables() {
    $('#dirTable').innerHTML = '';
    $('#cacheTable').innerHTML = '';
    $('#inflight').innerHTML = '';
    $('#note').textContent = '';
    $('#violation').classList.add('hidden');
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
