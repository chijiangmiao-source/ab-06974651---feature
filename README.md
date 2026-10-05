# 星载载荷 · 目录缓存重放复核器

总线短暂丢包后，复核员导入目录缓存事件记录，逐步或整段回放，确认**旧失效确认不会在新一轮写入中错误释放独占权限**。

## 规则模型

- 每轮写升级让目录为该缓存线推进一个**递增世代**（gen），本轮所有失效请求（`inv` 消息）绑定该世代。
- 只有**当前等待集合中的同世代确认**才能推进写入；等待集合清空时写入闭合，请求者获得独占（M）。
- **超时重传**只复制在途消息；**重复投递只能回放既有动作**（重新登记同世代待确认），不得产生新动作。
- 首次违约步即冻结，并说明涉及的线、核心与消息：

| 违约 | 含义 |
| --- | --- |
| `late-ack` 迟到确认 | 目录无等待写入 / 确认世代不符 / 确认者不在等待集合 |
| `missing-owner-data` 缺失拥有者数据 | 读缺失需要拥有者数据，但其副本无效（授权仍在途） |
| `duplicate-exclusive` 重复独占者 | 两个及以上核心同时持有 E/M 副本 |
| `directory-copy-mismatch` 目录与副本不一致 | 目录的拥有者/共享者记录与各核副本不符 |
| `stale-message` 过期消息 | 过期失效/授权到达，或重复投递越界产生新动作 |
| `invalid-event` 非法事件 | 越界引用、未知消息、目录忙时强行请求等 |

每步快照保留：目录（状态/拥有者/共享者/世代/等待集合）、各核缓存副本（状态/数据/已见失效世代/待确认世代）、在途消息（含重传副本的 `dupOf` 溯源）。

## 事件格式

```json
{
  "cores": 3,
  "lines": 1,
  "events": [
    { "type": "read_miss", "core": 0, "line": 0 },
    { "type": "deliver", "msg": 1 },
    { "type": "write_upgrade", "core": 1, "line": 0, "data": 7 },
    { "type": "timeout", "msg": 4 },
    { "type": "ack", "core": 0, "line": 0, "gen": 1 }
  ]
}
```

- 限制：≤4 核、≤8 缓存线、≤48 事件。
- 消息编号 `M1, M2, …` 按生成顺序确定；`deliver`/`timeout` 按编号引用在途消息。
- `write_upgrade` 可带 `data`（写入值）；`ack` 必须带 `gen`（确认世代）。

## 页面

- 计算全部在 **Web Worker** 中进行；每次导入/运行分配递增令牌，**取消或重新导入后旧结果不会覆盖新内容**。
- 导入内容存入 **localStorage 草稿**，重新打开页面即确定性重放同一份逐步证据。
- 支持单步、整段回放、暂停、重置、取消。

## 运行

### 本地

```bash
node server/server.js          # http://127.0.0.1:8080 （PORT 可改）
node --test tests/             # 规则测试
node scripts/build.js          # 页面构建（dist/）
SMOKE_BASE_URL=http://127.0.0.1:8080 node scripts/smoke.js
```

### Compose

```bash
HOST_PORT=8080 docker compose up --build --exit-code-from verify --abort-on-container-exit
```

- `web` 服务把回放页与健康响应暴露在宿主端口 `${HOST_PORT:-8080}`（`GET /`、`GET /health`）。
- `verify` 容器依次运行**规则测试 → 页面构建 → HTTP 冒烟**，完成后以退出码报告验收结果（0=通过，1=失败），上面的命令把该退出码透传为整条命令的退出码。

## 结构

```
app/protocol.js   协议核心（Worker 与 Node 测试共用）
app/worker.js     Web Worker 入口
app/main.js       页面逻辑（令牌防旧结果覆盖、localStorage 草稿）
app/index.html    回放页
server/server.js  静态服务 + /health
scripts/build.js  页面构建（语法校验 + 资源校验 + dist 清单）
scripts/smoke.js  HTTP 冒烟
scripts/verify.js 验收入口（测试+构建+冒烟，退出码报告）
tests/            规则测试（node:test）
```
