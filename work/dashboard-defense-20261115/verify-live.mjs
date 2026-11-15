import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { chromium } from 'file:///C:/Users/30256/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs'
const browser = await chromium.launch({ executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe', headless: true })
const out = 'work/dashboard-defense-20261115/screenshots'
let id
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 810 }, reducedMotion: 'reduce', acceptDownloads: true })
  const errors = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('http://localhost:4175/')
  await page.getByRole('button', { name: '运行实验', exact: true }).click()
  const dialog = page.locator('.detail-window')
  const name = dialog.getByLabel('结果目录名')
  id = 'verify-latency-' + Date.now()
  await name.fill(id)
  await dialog.getByLabel('键数', { exact: true }).fill('20000')
  await dialog.getByLabel('正式（秒）').fill('20')
  await dialog.getByRole('button', { name: '预览命令' }).click()
  await dialog.locator('.command-view').waitFor()
  assert.match(await dialog.locator('.command-view').textContent(), /'--keys' '20000'.*'--duration-seconds' '20'/)
  const exported = page.waitForEvent('download')
  await dialog.getByRole('button', { name: '下载 .sh' }).click()
  const script = await exported
  assert.match(await readFile(await script.path(), 'utf8'), /^#!\/usr\/bin\/env bash/)
  await dialog.getByRole('button', { name: '启动实验', exact: true }).click()
  await dialog.getByText('实验运行中', { exact: true }).waitFor()
  await dialog.getByRole('img', { name: '实时窗口记录曲线' }).waitFor({timeout:20000})
  await dialog.locator('.chart-empty-label').waitFor({ state: 'hidden', timeout: 20000 })
  await dialog.getByRole('checkbox', {name:'包含采样'}).check()
  await dialog.getByRole('button', {name:/指标采样/}).first().waitFor({ timeout: 8000 })
  await page.screenshot({path: `${out}/live-latency.png`})
  console.log('实时曲线与 SSE 采样已出现，等待', id)
  await page.waitForURL(url => url.searchParams.get('run') === id, { timeout: 100000 })
  const response = await page.request.get('/api/runs/' + id)
  const result = await response.json()
  assert.equal(result.status, 'success')
  assert.equal(result.manifest.config.key_count, 20000)
  assert.deepEqual(Object.values(result.summary.measurements).filter(value => value.target_rate).map(value => [value.target_rate, value.seconds]), [[500,20], [1000,20]])
  await page.getByRole('button', { name: '查看完整曲线', exact: false }).click()
  await dialog.getByRole('img', { name: /GET 阶段 p50 和 p99/ }).waitFor()
  await page.keyboard.press('Escape')
  await dialog.waitFor({ state: 'detached' })
  await page.getByRole('button', { name: '节点负载', exact: false }).filter({ hasText: '键数、操作数' }).click()
  await dialog.getByLabel('节点负载指标').waitFor()
  assert.equal(Object.keys(result.summary.measurements.rate_0.load_balance.per_node).length, 3)
  assert.deepEqual(errors, [])
  console.log('通过', id, '两档各 20 秒，工作台自动返回，负载曲线和节点分布已打开')
} finally {
  const sessions = await fetch('http://localhost:4175/api/experiments/current').then(r => r.json()).catch(() => ({}))
  if (id && sessions.session?.id === id && !sessions.session.endedAt) await fetch('http://localhost:4175/api/experiments/current/stop', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id })
  })
  await browser.close()
}
