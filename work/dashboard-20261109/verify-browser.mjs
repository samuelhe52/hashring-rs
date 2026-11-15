import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { chromium } from 'file:///C:/Users/30256/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs'

const browser = await chromium.launch({ executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe', headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, acceptDownloads: true })
const root = 'work/dashboard-20261109/'
const url = (process.env.DASHBOARD_TEST_URL || 'http://localhost:4174') + '/?run=my-latency-20261028-013354'
const errors = []
page.on('pageerror', error => errors.push(error.message))
await page.addInitScript(() => {
  window.barFrames = []
  document.addEventListener('animationstart', event => {
    if (event.animationName !== 'bar-enter' || !event.target.matches('.stage-track i')) return
    const bar = event.target
    const start = bar.getBoundingClientRect().width
    bar.addEventListener('animationend', () => window.barFrames.push([start, bar.getBoundingClientRect().width]), { once: true })
  })
})
try {
  await page.goto(url)
  await page.waitForFunction(() => window.barFrames.length >= 2)
  assert.ok((await page.evaluate(() => window.barFrames)).every(([start, end]) => start < end * .6))
  await page.emulateMedia({ reducedMotion: 'reduce' })
  assert.equal(await page.locator('.stage-track i').first().evaluate(el => getComputedStyle(el).animationDuration), '0s')
  await page.screenshot({ path: root + 'desktop.png', fullPage: true })
  const stage = page.locator('.stage-preview-row').nth(1)
  await stage.click()
  assert.equal(await stage.getAttribute('aria-pressed'), 'true')
  assert.match(await page.locator('.hero-readings').innerText(), /660/)

  const open = name => page.getByRole('button', { name }).click()
  const close = async () => {
    await page.keyboard.press('Escape')
    await page.locator('dialog').waitFor({ state: 'hidden' })
  }
  const caseButton = page.getByRole('button', { name: /^案例分析/ })
  await caseButton.click()
  await page.locator('.stage-comparison .data-chart svg').first().waitFor()
  assert.match(await page.locator('.case-delta').innerText(), /-72/)
  const scroll = await page.evaluate(() => {
    const stage = document.querySelector('.playback-stage')
    const body = document.querySelector('.window-body')
    const before = stage.getBoundingClientRect().top
    body.scrollTop = 270
    const after = stage.getBoundingClientRect().top
    const offset = body.scrollTop
    body.scrollTop = 0
    return { before, after, offset }
  })
  assert.ok(scroll.offset > 150 && Math.abs(scroll.before - scroll.after) < 24, JSON.stringify(scroll))
  await page.screenshot({ path: root + 'case-desktop.png' })
  for (const format of ['PNG', 'SVG']) {
    const [download] = await Promise.all([
      page.waitForEvent('download'),
      page.getByRole('button', { name: format, exact: true }).click(),
    ])
    await download.saveAs(root + 'case-get-p99.' + format.toLowerCase())
    assert.equal(await download.failure(), null)
  }
  const png = await readFile(root + 'case-get-p99.png')
  assert.equal(png.subarray(1, 4).toString(), 'PNG')
  assert.equal(png.readUInt32BE(16), 2200)
  const svg = await readFile(root + 'case-get-p99.svg', 'utf8')
  assert.match(svg, /my-latency-20261028-013354/)
  assert.match(svg, /732/)
  await open('实际速率')
  await page.waitForFunction(() => document.querySelector('.case-delta').textContent.includes('+500'))
  await open('初始哈希环')
  await page.locator('.ring-figure svg').waitFor()
  await close()
  assert.ok(await caseButton.evaluate(button => document.activeElement === button))

  await open(/^运行对比/)
  await page.locator('.comparison-deltas').waitFor()
  assert.match(await page.locator('.comparison-deltas').innerText(), /732/)
  await page.getByLabel('将所选运行作为重复组').check()
  assert.match(await page.locator('.repeat-summary').innerText(), /666.5/)
  await page.getByLabel('对比图类型').selectOption('group')
  await page.locator('.data-chart svg').first().waitFor()
  await page.locator('.window-body').evaluate(body => { body.scrollTop = 0 })
  await page.screenshot({ path: root + 'compare-desktop.png' })
  await page.getByLabel('对比图类型').selectOption('curve')
  await page.locator('.data-chart svg').first().waitFor()
  await page.mouse.click(12, 12)
  await page.locator('dialog').waitFor({ state: 'hidden' })

  await open(/^历史回放/)
  await page.getByRole('button', { name: '结束', exact: true }).click()
  await page.locator('.playback-plot .data-chart svg').first().waitFor()
  await page.getByLabel('回放图表').selectOption('rate')
  assert.match(await page.locator('.replay-legend').innerText(), /窗口成功速率/)
  await open('起点')
  await page.getByLabel('回放速度').selectOption('10')
  await open('播放')
  await page.waitForFunction(() => Number(document.querySelector('.replay-seek').value) > Number(document.querySelector('.replay-seek').min) + 2000)
  await open('暂停')
  await page.screenshot({ path: root + 'replay-desktop.png' })
  await close()

  await page.setViewportSize({ width: 390, height: 844 })
  await page.goto(url)
  await page.locator('.hero-readings').waitFor()
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth))
  await page.screenshot({ path: root + 'mobile.png', fullPage: true })
  await open('目录')
  await page.getByRole('link', { name: '运行记录', exact: true }).click()
  assert.equal(await page.evaluate(() => document.activeElement.id), 'records')
  await caseButton.click()
  await page.locator('.stage-comparison .data-chart svg').first().waitFor()
  assert.ok(await page.locator('.window-body').evaluate(body => body.scrollWidth <= body.clientWidth + 1))
  await page.screenshot({ path: root + 'case-mobile.png' })
  await close()
  await page.setViewportSize({ width: 720, height: 450 })
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth))
  const statePage = await browser.newPage()
  let finishLoading
  const loadingGate = new Promise(resolve => { finishLoading = resolve })
  await statePage.route('**/api/runs', async route => {
    await loadingGate
    await route.fulfill({ json: { runs: [] } })
  })
  await statePage.goto(url, { waitUntil: 'domcontentloaded' })
  await statePage.getByRole('heading', { name: '正在读取运行结果' }).waitFor()
  finishLoading()
  await statePage.getByText('当前没有可读取的实验结果', { exact: false }).waitFor()
  await statePage.unroute('**/api/runs')
  await statePage.route('**/api/runs', route => route.fulfill({ status: 503, json: { error: '验收用读取错误' } }))
  await statePage.reload()
  await statePage.getByRole('heading', { name: '结果列表读取失败' }).waitFor()
  await statePage.unroute('**/api/runs')
  await statePage.getByRole('button', { name: '重试读取', exact: true }).click()
  await statePage.locator('.hero-readings').waitFor()
  await statePage.close()
  assert.deepEqual(errors, [])
  console.log('桌面、手机、目录、键盘、固定动画区、条形与减少动效、对比、回放、PNG/SVG 下载和内容状态通过。')
} finally {
  await browser.close()
}
