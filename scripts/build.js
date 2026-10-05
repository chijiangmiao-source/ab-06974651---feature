/*
 * 页面构建：语法校验全部 JS、校验页面资源引用、复制 app/ 到 dist/ 并生成清单。
 * 任一检查失败以非零退出码结束。
 */
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFileSync } = require('child_process');

const repo = path.join(__dirname, '..');
const appDir = path.join(repo, 'app');
const distDir = path.join(repo, 'dist');

function fail(msg) {
  console.error(`[build] 失败：${msg}`);
  process.exit(1);
}

// 1. 语法校验
const jsDirs = ['app', 'server', 'scripts', 'tests'];
const jsFiles = [];
for (const d of jsDirs) {
  const dir = path.join(repo, d);
  if (!fs.existsSync(dir)) continue;
  for (const f of fs.readdirSync(dir)) {
    if (f.endsWith('.js')) jsFiles.push(path.join(dir, f));
  }
}
for (const f of jsFiles) {
  try {
    execFileSync(process.execPath, ['--check', f], { stdio: 'pipe' });
  } catch (err) {
    fail(`JS 语法错误 ${path.relative(repo, f)}\n${err.stderr || err.message}`);
  }
}
console.log(`[build] JS 语法校验通过（${jsFiles.length} 个文件）`);

// 2. 页面资源引用
const htmlPath = path.join(appDir, 'index.html');
if (!fs.existsSync(htmlPath)) fail('缺少 app/index.html');
const html = fs.readFileSync(htmlPath, 'utf8');
for (const ref of ['main.js', 'protocol.js', 'styles.css']) {
  if (!html.includes(ref)) fail(`index.html 未引用 ${ref}`);
  if (!fs.existsSync(path.join(appDir, ref))) fail(`缺少 app/${ref}`);
}
if (!fs.existsSync(path.join(appDir, 'worker.js'))) fail('缺少 app/worker.js');
if (!fs.readFileSync(path.join(appDir, 'worker.js'), 'utf8').includes('importScripts')) {
  fail('worker.js 未通过 importScripts 加载协议核心');
}
console.log('[build] 页面资源引用完整');

// 3. 复制到 dist 并生成清单
fs.rmSync(distDir, { recursive: true, force: true });
fs.mkdirSync(distDir, { recursive: true });
const manifest = { builtAt: new Date().toISOString(), files: [] };
for (const f of fs.readdirSync(appDir)) {
  const src = path.join(appDir, f);
  const buf = fs.readFileSync(src);
  fs.writeFileSync(path.join(distDir, f), buf);
  manifest.files.push({
    name: f,
    bytes: buf.length,
    sha256: crypto.createHash('sha256').update(buf).digest('hex'),
  });
}
fs.writeFileSync(path.join(distDir, 'manifest.json'), JSON.stringify(manifest, null, 2));
console.log(`[build] OK：dist/ 已生成（${manifest.files.length} 个页面文件）`);
