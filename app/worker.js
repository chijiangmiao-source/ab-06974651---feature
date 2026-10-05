/*
 * Web Worker：所有重放计算在此线程进行，主线程只负责渲染。
 * 每条消息携带 runToken，主线程据此丢弃已被取消/过期的结果。
 */
importScripts('protocol.js');

self.onmessage = function (e) {
  const data = e.data || {};
  const token = data.token;
  let payload;
  try {
    const result = self.Protocol.simulate(data.input);
    payload = { type: 'result', result };
  } catch (err) {
    payload = { type: 'error', error: String((err && err.message) || err) };
  }
  payload.token = token;
  self.postMessage(payload);
};
