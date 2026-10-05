/*
 * 静态服务器：提供回放页与 /health 健康响应。
 * 优先服务 dist/（构建产物），不存在时回退到 app/。
 */
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT || 8080);
const DIST = path.join(__dirname, '..', 'dist');
const APP = path.join(__dirname, '..', 'app');
const ROOT = fs.existsSync(path.join(DIST, 'index.html')) ? DIST : APP;

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');

  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify({ status: 'ok', service: 'dirreplay', root: path.basename(ROOT), time: new Date().toISOString() }));
    return;
  }

  let p;
  try {
    p = decodeURIComponent(url.pathname);
  } catch {
    res.writeHead(400); res.end('bad request'); return;
  }
  if (p === '/') p = '/index.html';
  const file = path.normalize(path.join(ROOT, p));
  if (!file.startsWith(ROOT + path.sep) && file !== ROOT) {
    res.writeHead(403); res.end('forbidden'); return;
  }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, {
      'Content-Type': TYPES[path.extname(file).toLowerCase()] || 'application/octet-stream',
      'Cache-Control': 'no-store',
    });
    res.end(data);
  });
});

server.listen(PORT, () => {
  console.log(`dirreplay web listening on :${PORT}, root=${ROOT}`);
});
