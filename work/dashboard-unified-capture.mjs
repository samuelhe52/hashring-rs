import { mkdir } from 'node:fs/promises'
import { chromium } from 'file:///C:/Users/30256/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs'
const folder = 'work/dashboard-unified-20261115'
await mkdir(folder, { recursive: true })
const browser = await chromium.launch({ executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe', headless: true })
try {
  for (const [width,height] of [[1440,810],[1280,720],[390,844],[320,780]]) {
    const page = await browser.newPage({ baseURL:'http://localhost:4173',viewport:{width,height},reducedMotion:'reduce' })
    page.on('pageerror', error => console.log(error))
    await page.goto('/?run=my-latency-20261028-013354')
    await page.getByRole('img',{name:/GET p50、p99 延迟/}).waitFor()
    await page.screenshot({ path: folder+'/latency-'+width+'.png', fullPage: width<500 })
    const size = await page.evaluate(() => [document.documentElement.scrollWidth,document.documentElement.clientWidth,
      ...['.dashboard-context','.dashboard-readings','.dashboard-pair','.dashboard-resources'].map(selector => document.querySelector(selector).getBoundingClientRect().bottom)])
    console.log('latency',width,size.map(Math.round).join(', '))
    await page.locator('.dashboard-actions').getByRole('button',{name:/切换运行/}).click()
    const modal = page.locator('.detail-window')
    await modal.getByRole('button',{name:/主节点故障/}).waitFor()
    if (width === 1440 || width ===390) await page.screenshot({ path: folder+'/runs-'+width+'.png' })
    await modal.getByRole('button',{name:/主节点故障/}).click()
    await page.getByRole('heading',{name:'终止主节点进程'}).waitFor()
    await page.screenshot({ path: folder+'/fault-'+width+'.png', fullPage:width<500 })
    console.log('fault',width,await page.getByRole('button',{name:/下一阶段/}).boundingBox())
    if (width === 1440) {
      await page.locator('.dashboard-actions').getByRole('button',{name:/切换运行/}).click()
      await modal.getByRole('button',{name:/在线扩容/}).click()
      await page.getByRole('heading',{name:'扩容前的拓扑'}).waitFor()
      await page.getByRole('button',{name:/迁移校验/}).click()
      await page.screenshot({ path: folder+'/expansion-1440.png' })
    }
    await page.close()
  }
} finally { await browser.close() }
