/*
 * HTTP 冒烟：检查回放页、健康响应与关键静态资源。
 * 目标地址由 SMOKE_BASE_URL 指定（默认 http://127.0.0.1:8080）。
 */
'use strict';

const BASE = (process.env.SMOKE_BASE_URL || 'http://127.0.0.1:8080').replace(/\/+$/, '');
const TIMEOUT_MS = 5000;

async function get(p) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(BASE + p, { signal: ctrl.signal });
    const text = await res.text();
    return { status: res.status, text };
  } finally {
    clearTimeout(t);
  }
}

const checks = [
  ['GET / 回放页', async () => {
    const r = await get('/');
    if (r.status !== 200) return `状态码 ${r.status}`;
    if (!r.text.includes('目录缓存重放复核器')) return '页面缺少标题标记';
    return null;
  }],
  ['GET /health 健康响应', async () => {
    const r = await get('/health');
    if (r.status !== 200) return `状态码 ${r.status}`;
    let body;
    try { body = JSON.parse(r.text); } catch { return '响应不是合法 JSON'; }
    if (body.status !== 'ok') return `status=${body.status}`;
    return null;
  }],
  ['GET /main.js', async () => (await get('/main.js')).status === 200 ? null : '非 200'],
  ['GET /worker.js', async () => (await get('/worker.js')).status === 200 ? null : '非 200'],
  ['GET /protocol.js', async () => (await get('/protocol.js')).status === 200 ? null : '非 200'],
  ['GET /styles.css', async () => (await get('/styles.css')).status === 200 ? null : '非 200'],
];

(async () => {
  let failed = 0;
  for (const [name, fn] of checks) {
    try {
      const err = await fn();
      if (err) { failed++; console.log(`[smoke] ✗ ${name}：${err}`); }
      else console.log(`[smoke] ✓ ${name}`);
    } catch (e) {
      failed++;
      console.log(`[smoke] ✗ ${name}：${e.message}`);
    }
  }
  if (failed) {
    console.log(`[smoke] 失败：${failed}/${checks.length} 项未通过（目标 ${BASE}）`);
    process.exit(1);
  }
  console.log(`[smoke] OK：${checks.length} 项全部通过（目标 ${BASE}）`);
})();
