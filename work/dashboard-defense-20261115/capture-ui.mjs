import { chromium } from 'file:///C:/Users/30256/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs'
import { mkdir } from 'node:fs/promises'
const out = 'work/dashboard-defense-20261115/screenshots'
await mkdir(out, { recursive: true })
const browser = await chromium.launch({ executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe', headless: true })
try {
  for (const [width, height] of [[1440,810], [1280,720], [390,844]]) {
    const page = await browser.newPage({ viewport: { width, height }, reducedMotion: 'reduce' })
    page.on('pageerror', error => console.error(error))
    await page.goto('http://localhost:4173/?view=defense')
    await page.getByRole('heading', { name: '终止主节点进程' }).waitFor()
    await page.screenshot({ path: `${out}/fault-${width}.png`, fullPage: width < 500 })
    const items = await page.evaluate(() => Object.fromEntries(['.defense-header','.defense-readings','.defense-figure','.defense-evidence','.defense-step-controls','.defense-resources','.defense-index .index-rail'].map(selector => {
      const box = document.querySelector(selector).getBoundingClientRect()
      return [selector, [box.x, box.y, box.width, box.bottom].map(Math.round)]
    })))
    console.log(width, JSON.stringify(items), 'scrollWidth', await page.evaluate(() => document.documentElement.scrollWidth))
    if (width === 1440) {
      await page.getByRole('button', {name:'下一阶段'}).click()
      await page.getByRole('button', {name:'下一阶段'}).click()
      await page.getByRole('button', {name:'下一阶段'}).click()
      await page.screenshot({ path: `${out}/fault-restored.png` })
      await page.getByRole('button', {name:'在线扩容', exact: true}).click()
      await page.getByRole('heading', {name:'扩容前的拓扑'}).waitFor()
      await page.getByRole('button', {name:/迁移校验/}).click()
      await page.screenshot({ path: `${out}/expansion-1440.png` })
      await page.getByRole('button', {name:'机制示意', exact:true}).click()
      await page.screenshot({ path: `${out}/expansion-mechanism.png` })
      await page.getByRole('button', {name:'正式负载曲线', exact:true}).first().click()
      await page.getByRole('img', { name: /GET 延迟—实际速率/ }).waitFor()
      await page.screenshot({ path: `${out}/formal-load.png` })
      await page.getByRole('button', {name:'物理节点键数', exact:true}).first().click()
      await page.getByRole('img', { name: /各物理节点拥有的键数/ }).waitFor()
      await page.screenshot({ path: `${out}/balance.png` })
      await page.keyboard.press('Escape')
      await page.locator('.detail-window').waitFor({ state: 'detached' })
      await page.getByRole('button', {name:'验收对照', exact:true}).first().click()
      await page.locator('.defense-acceptance-table').waitFor()
      await page.screenshot({ path: `${out}/acceptance.png` })
    }
    await page.close()
  }
} finally { await browser.close() }
