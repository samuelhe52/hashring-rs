import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { chromium } from 'file:///C:/Users/30256/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs'

const url = 'http://localhost:4175'
const id = 'verify-browser-' + Date.now()
const browser = await chromium.launch({ executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe', headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, acceptDownloads: true })
const artifacts = 'work/dashboard-20261109/'
try {
  const { runs } = await (await fetch(url + '/api/runs')).json()
  const stopped = runs.find(run => run.id === 'verify-stop-1794158675872')
  assert.equal(stopped.status, 'incomplete')
  await page.goto(url + '/?run=' + stopped.id)
  await page.getByText('结果尚不完整', { exact: false }).waitFor()
  const btn = name => page.getByRole('button', { name, exact: true })
  await btn('运行实验').click()
  if ((await (await fetch(url + '/api/experiments/current')).json()).session) await btn('配置新实验').click()
  await page.getByLabel('结果目录名', { exact: true }).fill(id)
  await page.getByLabel('目标速率（ops/s，逗号分隔）').fill('120,240')
  await page.getByLabel('预热（秒）').fill('1')
  await page.getByLabel('正式（秒）').fill('4')
  await btn('预览命令').click()
  await page.locator('.command-view').waitFor()
  assert.match(await page.locator('.command-view').innerText(), /'--target-rates' '120,240'/)
  const [download] = await Promise.all([page.waitForEvent('download'), btn('下载 .sh').click()])
  await download.saveAs(artifacts + 'launch-example.sh')
  assert.match(await readFile(artifacts + 'launch-example.sh', 'utf8'), /120,240/)
  await btn('启动实验').click()
  await page.locator('.experiment-banner').getByText(id, { exact: true }).waitFor()
  await page.getByLabel('实时图表', { exact: true }).waitFor({ timeout: 45000 })
  await page.getByLabel('实时图表', { exact: true }).selectOption('rate')
  await page.waitForFunction(() => document.querySelector('.replay-readings').textContent.includes('120'))
  await page.screenshot({ path: artifacts + 'live-desktop.png' })
  await page.locator('dialog').waitFor({ state: 'hidden', timeout: 45000 })
  await page.waitForURL(url + '/?run=' + id)
  await page.locator('.hero-readings').waitFor()
  assert.match(await page.locator('.result-context').innerText(), new RegExp(id))
  console.log('生产页面通过表单启动，脚本已下载，实时曲线更新，成功后打开新结果：' + id)
  for (const [name, heading] of [['verify-correctness-1794158675872', '扩缩容案例'], ['verify-availability-1794158675872', '故障恢复案例']]) {
    await page.goto(url + '/?run=' + name)
    await page.getByRole('button', { name: /^案例分析/ }).click()
    await page.getByRole('heading', { name: heading }).waitFor()
    await page.locator('.ring-figure svg').waitFor()
    assert.ok(await page.locator('.scenario-facts').isVisible())
  }
  console.log('生产页面可浏览停止的部分结果，迁移和故障案例从真实记录展示环与耗时。')
} finally {
  const { session } = await (await fetch(url + '/api/experiments/current')).json()
  if (session?.id === id && session.endedAt === null) {
    await fetch(url + '/api/experiments/current/stop', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id }),
    })
  }
  await browser.close()
}
