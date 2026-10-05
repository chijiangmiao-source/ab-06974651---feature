/*
 * 验收入口（verify 容器）：依次运行规则测试、页面构建、HTTP 冒烟，
 * 全部完成后再以退出码报告验收结果（0=通过，1=失败）。
 */
'use strict';

const { spawnSync } = require('child_process');

const steps = [
  ['规则测试', ['--test', 'tests/']],
  ['页面构建', ['scripts/build.js']],
  ['HTTP 冒烟', ['scripts/smoke.js']],
];

let failed = 0;
for (const [name, args] of steps) {
  console.log(`\n=== ${name} ===`);
  const r = spawnSync(process.execPath, args, { stdio: 'inherit', env: process.env });
  if (r.status !== 0) {
    failed++;
    console.log(`[verify] ${name} 未通过（退出码 ${r.status}）`);
  } else {
    console.log(`[verify] ${name} 通过`);
  }
}

console.log(failed === 0
  ? '\n[verify] 验收通过：规则测试、页面构建、HTTP 冒烟全部成功'
  : `\n[verify] 验收失败：${failed} 项未通过`);
process.exit(failed === 0 ? 0 : 1);
