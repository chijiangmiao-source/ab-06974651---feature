/*
 * Web Worker：所有重放计算在此线程进行，主线程只负责渲染。
 * 每条消息携带 runToken，主线程据此丢弃已被取消/过期的结果：
 *   - 整段重放（simulate）
 *   - 因果链回溯（buildChain）与可回溯对象清单（listTraceables）
 * 链路查询对同一份输入确定性地重放后再取证，保证链上证据与当前导入一致。
 */
importScripts('protocol.js');

self.onmessage = function (e) {
  const data = e.data || {};
  const token = data.token;
  const op = data.op || 'run';
  let payload;
  try {
    const result = self.Protocol.simulate(data.input);
    if (op === 'traceables') {
      payload = { type: 'traceables', traceables: self.Protocol.listTraceables(result) };
    } else if (op === 'chain') {
      payload = {
        type: 'chain',
        chain: result.ok ? self.Protocol.buildChain(result, data.query) : { ok: false, error: result.error },
      };
    } else {
      payload = { type: 'result', result, traceables: self.Protocol.listTraceables(result) };
    }
  } catch (err) {
    payload = { type: 'error', error: String((err && err.message) || err) };
  }
  payload.token = token;
  payload.op = op;
  payload.reqId = data.reqId || null;
  self.postMessage(payload);
};
