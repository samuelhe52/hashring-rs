import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { chromium } from 'file:///C:/Users/30256/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs'

const origin = process.env.DASHBOARD_TEST_URL || 'http://localhost:4174'
const url = origin + '/?run=my-latency-20261028-013354'
const folder = 'work/dashboard-skill-redesign-20261109/'
const browser = await chromium.launch({ executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe', headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, acceptDownloads: true })
page.setDefaultTimeout(10000)
const issues = []
page.on('pageerror', error => issues.push(error.message))
const button = name => page.getByRole('button', { name, exact: true })
const panel = page.locator('.detail-window')
const siteMenu = page.locator('.workbench .index-dialog')
const siteToggle = page.locator('.workbench .index-toggle')
const panelMenu = panel.locator('.index-dialog')
const focus = () => page.evaluate(() => document.activeElement.id)
const navLink = (nav, name) => nav.getByRole('link', { name, exact: true })

async function load() {
  await page.goto(url)
  await page.locator('.hero-readings').waitFor()
}

try {
  await load()
  const rail = page.locator('.workbench .index-rail')
  await rail.waitFor()
  assert.equal(await rail.evaluate(el => getComputedStyle(el).position), 'fixed')
  assert.equal(await rail.evaluate(el => getComputedStyle(el).backgroundColor), 'rgba(0, 0, 0, 0)')
  assert.equal(await siteToggle.isVisible(), false)
  await navLink(rail, '分析与演示').click()
  assert.equal(await focus(), 'analysis')
  assert.equal(await navLink(rail, '分析与演示').getAttribute('aria-current'), 'location')
  await navLink(rail, '运行记录').click()
  assert.equal(await focus(), 'records')
  assert.equal(await navLink(rail, '运行记录').getAttribute('aria-current'), 'location')
  await navLink(rail, '运行概览').click()
  await page.locator('.stage-preview-row').nth(1).click()
  assert.match(await page.locator('.hero-readings').innerText(), /660/)
  await page.locator('.stage-preview-row').first().click()
  await page.screenshot({ path: folder + 'dashboard-desktop.png', animations: 'disabled' })

  const caseButton = page.getByRole('button', { name: /^案例分析/ })
  await caseButton.click()
  await panel.locator('.stage-comparison .data-chart svg').first().waitFor()
  await page.waitForFunction(() => getComputedStyle(document.querySelector('.detail-window')).transform === 'none')
  const widths = await panel.locator('.stage-comparison').evaluate(async el => {
    const width = () => el.querySelector('path[fill="#b4532a"], path[fill="#2d7774"]')?.getBBox().width ?? null
    const frame = () => new Promise(requestAnimationFrame)
    for (let i = 0; i < 35; i++) await frame()
    const values = [width()]
    el.querySelector('.segmented button:nth-child(2)').click()
    for (let i = 0; i < 34; i++) { await frame(); values.push(width()) }
    return values
  })
  assert.notEqual(widths[0], null, 'chart SVG bar')
  const [lo, hi] = [widths[0], widths.at(-1)].sort((a, b) => a - b)
  assert.ok(hi - lo > 40 && widths.some(width => width > lo + 3 && width < hi - 3), JSON.stringify(widths))
  assert.match(await panel.locator('.case-delta').innerText(), /\+500/)
  const sticky = await panel.evaluate(async dialog => {
    const stage = dialog.querySelector('.playback-stage'), body = dialog.querySelector('.window-body')
    const before = stage.getBoundingClientRect().top
    body.scrollTop = 210
    await new Promise(requestAnimationFrame)
    return [before, stage.getBoundingClientRect().top, body.scrollTop]
  })
  assert.ok(sticky[2] > 130 && Math.abs(sticky[1] - sticky[0]) <= 23, JSON.stringify(sticky))
  await navLink(panel.locator('.index-rail'), '案例').click()
  await page.waitForFunction(() => document.querySelector('.detail-window .window-body').scrollTop <= 18)
  await page.screenshot({ path: folder + 'case-desktop.png', animations: 'disabled' })
  const [exportFile] = await Promise.all([page.waitForEvent('download'), button('SVG').click()])
  const exportSvg = await readFile(await exportFile.path(), 'utf8')
  assert.match(exportSvg, /my-latency-20261028-013354/)
  assert.match(exportSvg, /1,000/)
  await page.keyboard.press('Escape')
  await panel.waitFor({ state: 'hidden' })
  assert.ok(await caseButton.evaluate(element => document.activeElement === element))

  await page.setViewportSize({ width: 1024, height: 780 })
  await siteToggle.click()
  await siteMenu.waitFor()
  await page.waitForFunction(() => getComputedStyle(document.querySelector('.workbench .index-dialog')).transform === 'none')
  const drawer = await siteMenu.boundingBox()
  assert.ok(drawer.x > 600 && drawer.y < 115 && drawer.height > 580, JSON.stringify(drawer))
  await page.screenshot({ path: folder + 'directory-drawer.png' })
  await page.mouse.click(42, 410)
  await siteMenu.waitFor({ state: 'hidden' })

  await page.setViewportSize({ width: 390, height: 844 })
  await load()
  await siteToggle.waitFor()
  const ring = siteToggle.locator('.index-progress-value')
  const startOffset = Number(await ring.getAttribute('stroke-dashoffset'))
  await page.evaluate(() => scrollTo(0, 160))
  await page.waitForFunction(start => Number(document.querySelector('.workbench .index-progress-value').getAttribute('stroke-dashoffset')) < start, startOffset)
  const screen = await siteToggle.boundingBox()
  assert.ok(screen.x + screen.width > 350 && screen.y > 760 && Math.abs(screen.width - screen.height) < 1, JSON.stringify(screen))
  await siteToggle.focus()
  await page.keyboard.press('Enter')
  await siteMenu.waitFor()
  await page.waitForFunction(() => getComputedStyle(document.querySelector('.workbench .index-dialog')).transform === 'none')
  assert.match(await page.evaluate(() => document.activeElement.className || document.activeElement.tagName), /A/)
  const sheet = await siteMenu.boundingBox()
  assert.ok(sheet.width >= 350 && sheet.y > 500 && sheet.y + sheet.height < 844, JSON.stringify(sheet))
  await page.screenshot({ path: folder + 'directory-sheet.png' })
  await navLink(siteMenu, '运行记录').click()
  await siteMenu.waitFor({ state: 'hidden' })
  await page.waitForFunction(() => scrollY > 350 && document.activeElement.id === 'records')
  await siteToggle.click()
  await siteMenu.waitFor()
  assert.equal(await navLink(siteMenu, '运行记录').getAttribute('aria-current'), 'location')
  await page.keyboard.press('Escape')
  await siteMenu.waitFor({ state: 'hidden' })
  assert.ok(await siteToggle.evaluate(element => document.activeElement === element))

  await page.emulateMedia({ reducedMotion: 'reduce' })
  await caseButton.click()
  await panel.locator('.stage-comparison').waitFor()
  await panel.locator('.index-toggle').click()
  await panelMenu.waitFor()
  assert.equal(await panelMenu.evaluate(el => getComputedStyle(el).transitionDuration), '0s')
  await page.keyboard.press('Escape')
  await panelMenu.waitFor({ state: 'hidden' })
  assert.ok(await panel.isVisible())
  await panel.locator('.index-toggle').click()
  await navLink(panelMenu, '事件').click()
  await panelMenu.waitFor({ state: 'hidden' })
  assert.equal(await focus(), 'playback-events')
  assert.ok(await panel.locator('.window-body').evaluate(el => el.scrollTop > 0 && el.scrollWidth <= el.clientWidth + 1))
  await page.keyboard.press('Escape')
  await panel.waitFor({ state: 'hidden' })
  for (const width of [320, 390, 720]) {
    await page.setViewportSize({ width, height: 844 })
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth))
  }
  await page.setViewportSize({ width: 390, height: 844 })
  await load()
  await page.screenshot({ path: folder + 'dashboard-mobile.png', fullPage: true, animations: 'disabled' })
  assert.deepEqual(issues, [])
  console.log(JSON.stringify({ referenceLayouts: ['fixed rail', 'right drawer', 'bottom sheet'], directoryProgressStart: startOffset, interpolatedBar: [lo, hi], pinnedPlot: sticky, browserErrors: issues }))
} finally {
  await browser.close()
}
