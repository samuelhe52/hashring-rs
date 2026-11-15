import assert from 'node:assert/strict'
import { mkdir, readFile } from 'node:fs/promises'
import { chromium } from 'file:///C:/Users/30256/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs'
import { createStore } from '../dashboard/server/store.mjs'

const out = 'work/dashboard-unified-20261115'
await mkdir(out, { recursive: true })
const browser = await chromium.launch({ executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe', headless: true })
const errors = []
const create = async (port = 4173, width = 1440, height = 810) => {
  const page = await browser.newPage({ baseURL: 'http://localhost:' + port, viewport: { width, height }, acceptDownloads: true, reducedMotion: 'reduce' })
  page.on('pageerror', error => errors.push(error.message))
  page.setDefaultTimeout(9000)
  return page
}
const modal = page => page.locator('dialog.detail-window')
const readings = page => page.locator('.dashboard-readings dd strong').allTextContents()
const stage = page => page.locator('.case-stage-line [aria-current="step"]')
const heading = (page, name) => page.getByRole('heading', { name, exact: true }).waitFor()
const shot = (page, name, fullPage = false) => page.screenshot({ path: out + '/' + name + '.png', fullPage })
const choose = (page, name) => modal(page).locator('.run-card').filter({ hasText: name }).click()
const openPicker = page => page.locator('.dashboard-actions').getByRole('button', { name: /切换运行/ }).click()
const openExperiment = page => page.locator('.dashboard-actions').getByRole('button', { name: '运行实验', exact: true }).click()
const close = async page => {
  await page.keyboard.press('Escape')
  await modal(page).waitFor({ state: 'detached' })
}

let liveId
try {
  const page = await create()
  const list = await (await page.request.get('/api/runs?include=demo')).json()
  const latest = list.runs.find(run => !run.source)
  await page.goto('/')
  await heading(page, latest.name)
  assert.equal(new URL(page.url()).searchParams.get('run'), latest.id)
  assert.equal(new URL(page.url()).searchParams.has('view'), false)
  assert.equal(await page.getByRole('link', {name: /答辩演示|实验工作台/}).count(), 0)
  await shot(page, 'dashboard-1440')

  await openPicker(page)
  await modal(page).getByLabel('筛选运行来源').selectOption('demo')
  assert.equal(await modal(page).locator('.run-card').count(), 3)
  await shot(page, 'samples-1440')
  await choose(page, '主节点故障')
  await heading(page, '终止主节点进程')
  assert.equal(new URL(page.url()).searchParams.has('view'), false)
  assert.deepEqual(await readings(page), ['5.19', '10.30', '128 / 128'])
  await shot(page, 'fault-1440')
  for (const marker of ['5.13','5.19','10.30']) {
    await page.getByRole('button', { name: /下一阶段/ }).click()
    assert.match(await stage(page).textContent(), new RegExp(marker.replace('.', '\\.')))
  }
  const current = await page.locator('.case-scrubber output').textContent()
  assert.match(await page.locator('.case-figure footer').textContent(), /已发布 epoch 4.*epoch 2/)
  const record = page.getByRole('button', { name: /写探针自身耗时.*事件第 16 行/ })
  await record.click()
  await modal(page).getByText(/events.jsonl.*第 16 行/).waitFor()
  await close(page)
  assert.equal(await record.evaluate(el => document.activeElement === el), true)
  await openExperiment(page)
  await modal(page).getByRole('button', { name: '预览命令', exact: true }).waitFor()
  await close(page)
  assert.equal(await page.locator('.case-scrubber output').textContent(), current)
  await page.getByRole('button', { name: /快照详情/ }).click()
  assert.equal(await modal(page).getByLabel('选择拓扑快照').inputValue(), '5')
  await close(page)

  await openPicker(page)
  await choose(page, '在线扩容')
  await heading(page, '扩容前的拓扑')
  assert.deepEqual(await readings(page), ['2,000','172','171'])
  await page.getByRole('button', { name: /迁移校验/ }).click()
  assert.match(await page.locator('.case-figure footer').textContent(), /扩容后的完整快照未记录/)
  await shot(page, 'expansion-1440')
  await page.getByRole('button', { name: '机制示意', exact: true }).click()
  assert.match(await page.locator('.case-ring-number').textContent(), /D/)
  await page.getByRole('button', { name: '加入前', exact: true }).click()
  assert.match(await page.locator('.case-ring-number').textContent(), /B/)
  await page.locator('.dashboard-resources').getByRole('button', { name: '实验配置', exact: true }).click()
  await modal(page).getByText('样例复跑命令', {exact:true}).waitFor()
  await close(page)
  await page.locator('.dashboard-source-links').getByRole('button', { name: '文件与日志', exact: true }).click()
  await modal(page).getByRole('button', { name: '进程日志', exact: true }).first().click()
  await modal(page).getByLabel('选择文件').selectOption('coordinator.stdout.log')
  await modal(page).getByText(/coordinator ready/).waitFor()
  await close(page)

  await openPicker(page)
  await modal(page).getByLabel('筛选运行来源').selectOption('local')
  await modal(page).getByLabel('搜索运行名称').fill('my-latency')
  await choose(page, 'my-latency-20261028-013354')
  await heading(page, 'my-latency-20261028-013354')
  assert.deepEqual(await readings(page), ['500', '401', '732'])
  await page.getByRole('img', { name: /主节点键数节点分布/ }).waitFor()
  assert.match(await page.locator('.dashboard-load-card .data-chart').textContent(), /635.*534.*831/s)
  await page.locator('.dashboard-stage-select select').selectOption('rate_1')
  assert.equal((await readings(page))[2], '660')
  await page.getByRole('navigation', { name: '曲线操作类型' }).getByRole('button', { name: 'PUT', exact: true }).click()
  await page.getByRole('button', { name: /延迟明细与导出/ }).click()
  await modal(page).getByRole('img', { name: /PUT 阶段 p50/ }).waitFor()
  const [download] = await Promise.all([page.waitForEvent('download'), modal(page).getByRole('button', { name: 'SVG', exact: true }).click()])
  await download.saveAs(out + '/latency-put.svg')
  const exported = await readFile(out + '/latency-put.svg', 'utf8')
  assert.match(exported, /PUT 阶段 p50/)
  assert.match(exported, /my-latency-20261028-013354/)
  await close(page)
  await page.getByLabel('节点负载指标').selectOption('cpu_percent')
  await page.getByRole('button', { name: /节点明细与导出/ }).click()
  assert.equal(await modal(page).getByLabel('节点负载指标').inputValue(), 'cpu_percent')
  await close(page)
  await page.locator('.dashboard-resources').getByRole('button', { name: '跨运行对比', exact: true }).click()
  await modal(page).getByLabel('对比图类型').selectOption('curve')
  await modal(page).getByRole('img', { name: /GET p99 延迟对比/ }).waitFor()
  await close(page)
  await page.locator('.dashboard-source-links').getByRole('button', { name: '验收对照', exact: true }).click()
  assert.match(await modal(page).locator('.reference-acceptance-table').textContent(), /64,761.*19.3%/s)
  await close(page)
  await page.locator('.dashboard-source-links').getByRole('button', { name: '正式负载曲线', exact: true }).click()
  await modal(page).getByRole('img', { name: /GET 延迟—实际速率/ }).waitFor()
  await close(page)
  console.log('同一 URL 的运行切换、两类回放、目录弹窗、拓扑证据、导出和对比通过')

  const oldRun = 'verify-performance-1794679754597'
  const store = createStore('work/dashboard-20261109/verification-results')
  const perf = await store.readRun(oldRun)
  const performance = await create()
  await performance.route('**/api/runs?include=demo', route => route.fulfill({ json: {
    runs: [{ id: perf.id, name: perf.name, status: perf.status, config: perf.manifest.config, warnings: perf.warnings }],
  } }))
  await performance.route('**/api/runs/' + oldRun, route => route.fulfill({json:perf}))
  await performance.goto('/?run=' + oldRun)
  await heading(performance, '批量吞吐')
  await heading(performance, '空闲基线')
  await shot(performance, 'performance-1440')
  await performance.close()
  console.log('旧 performance 原始结果的吞吐与空闲基线通过')

  for (const [width,height] of [[1280,720],[390,844],[320,780]]) {
    const narrow = await create(4173, width, height)
    await narrow.goto('/?run=my-latency-20261028-013354')
    await narrow.getByRole('img', {name: /GET p50、p99 延迟/}).waitFor()
    assert.ok(await narrow.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth))
    await shot(narrow, 'latency-' + width, width < 500)
    if (width < 500) {
      const dir = narrow.locator('.dashboard-index').getByRole('button',{name:'打开完整目录'})
      await dir.focus()
      await narrow.keyboard.press('Enter')
      await narrow.locator('dialog.index-dialog[open]').waitFor()
      await narrow.keyboard.press('Escape')
      await narrow.locator('dialog.index-dialog[open]').waitFor({ state:'hidden' })
    }
    await narrow.goto('/?run=' + encodeURIComponent('@demo/verify-availability-1794158675872'))
    await heading(narrow,'终止主节点进程')
    if (width >= 500) {
      const next = await narrow.getByRole('button',{name:/下一阶段/}).boundingBox()
      assert.ok(next.y + next.height <= height)
    }
    await shot(narrow, 'fault-' + width, width < 500)
    await narrow.close()
  }
  console.log('1280×720 和 390／320 px 的投影控制、窄屏目录、数据区通过')

  const live = await create(4177)
  await live.goto('/')
  await heading(live, '终止主节点进程')
  await openExperiment(live)
  liveId = 'verify-unified-' + Date.now()
  await modal(live).getByLabel('结果目录名').fill(liveId)
  await modal(live).getByLabel('正式（秒）').fill('4')
  await modal(live).getByLabel('预热（秒）').fill('1')
  await modal(live).getByRole('button', { name: '预览命令', exact:true }).click()
  await modal(live).getByRole('button', { name: '启动实验', exact:true }).click()
  await modal(live).getByText('实验运行中',{exact:true}).waitFor()
  await modal(live).getByRole('img', { name: '实时窗口记录曲线' }).waitFor({ timeout: 16000 })
  await modal(live).locator('.chart-empty-label').waitFor({ state: 'hidden', timeout: 16000 })
  await shot(live, 'live-1440')
  await live.waitForURL(url => url.searchParams.get('run') === liveId, { timeout: 25000 })
  await heading(live, liveId)
  await live.getByRole('img', { name: /主节点键数节点分布/ }).waitFor()
  assert.equal(await modal(live).count(), 0)
  const finished = await (await live.request.get('/api/runs/' + liveId)).json()
  assert.equal(finished.status,'success')
  assert.deepEqual(Object.values(finished.summary.measurements).filter(value => value.target_rate).map(value => [value.target_rate, value.seconds]), [[500,4],[1000,4]])
  assert.deepEqual(errors, [])
  console.log('真实实验实时窗口与完成后自动切换运行通过', liveId)
} finally {
  if (liveId) {
    const base = 'http://localhost:4177/api/experiments/current'
    const response = await fetch(base).then(result => result.json()).catch(() => ({}))
    if (response.session?.id === liveId && !response.session.endedAt) await fetch(base + '/stop', {
      method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({id:liveId}),
    })
  }
  await browser.close()
}

