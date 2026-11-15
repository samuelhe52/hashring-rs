import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { createDashboardServer } from './index.mjs'
import { fixture, manifest, partial } from './test-fixture.mjs'

test('HTTP 可读取嵌套运行与原文，拒绝写入，失败请求不会破坏后续读取', async t => {
  const f = await fixture(t)
  const dir = await f.writeRun('campaign/run', manifest, partial)
  const before = await readFile(path.join(dir, 'summary.json'), 'utf8')
  const app = await createDashboardServer({ resultsRoot: f.root })
  await new Promise(resolve => app.server.listen(0, '127.0.0.1', resolve))
  t.after(() => app.close())
  const base = `http://127.0.0.1:${app.server.address().port}`
  const id = encodeURIComponent('campaign/run')
  assert.equal((await (await fetch(`${base}/api/runs`)).json()).runs.length, 1)
  const detail = await (await fetch(`${base}/api/runs/${id}`)).json()
  assert.equal(detail.summary.measurements.rate_0.latency_us.get.p99, 732)
  assert.equal(detail.status, 'failed')
  assert.equal(await (await fetch(`${base}/api/runs/${id}/raw?file=summary.json`)).text(), before)
  const download = await fetch(`${base}/api/runs/${id}/raw?file=summary.json&download=1`)
  assert.match(download.headers.get('content-disposition'), /^attachment;.*summary\.json/)
  assert.equal(await download.text(), before)
  assert.equal((await fetch(`${base}/api/runs`, { method: 'POST', body: '{}' })).status, 405)
  const previewUrl = `${base}/api/experiments/preview`
  const validConfig = { kind: 'yaml', name: 'preview-only', yaml: 'mode: latency\n' }
  const preview = (body, origin = base) => fetch(previewUrl, {
    method: 'POST', headers: { 'Content-Type': 'application/json', Origin: origin }, body: JSON.stringify(body),
  })
  assert.equal((await preview(validConfig, 'https://example.com')).status, 403)
  assert.equal((await preview({ ...validConfig, name: '../escape' })).status, 400)
  const plan = await preview(validConfig)
  assert.equal(plan.status, 200)
  assert.match((await plan.json()).script, /mode: latency/)
  assert.equal((await fetch(`${base}/api/runs/preview-only`)).status, 404)
  assert.equal((await fetch(`${base}/api/runs/${id}/raw?file=coordinator.redb`)).status, 400)
  assert.equal((await fetch(`${base}/api/runs/missing`)).status, 404)
  assert.equal((await fetch(`${base}/api/runs/%`)).status, 400)
  assert.equal((await fetch(`${base}/api/runs/${id}`)).status, 200)
  assert.equal(await readFile(path.join(dir, 'summary.json'), 'utf8'), before)
})

test('事件接口返回无损 u64、原文和异常行；缺事件文件不是运行不存在', async t => {
  const f = await fixture(t)
  const dir = await f.writeRun('campaign/run', manifest, partial)
  const raw = '{"event":"topology_published","unix_ms":1000,"fields":{"token":18446744073709551615}}'
  await writeFile(path.join(dir, 'events.jsonl'), raw + '\n{unfinished')
  await f.writeRun('no-events', manifest)
  const app = await createDashboardServer({ resultsRoot: f.root })
  await new Promise(resolve => app.server.listen(0, '127.0.0.1', resolve))
  t.after(() => app.close())
  const base = `http://127.0.0.1:${app.server.address().port}/api/runs/`
  const response = await fetch(base + encodeURIComponent('campaign/run') + '/events')
  assert.equal(response.status, 200)
  const result = await response.json()
  assert.equal(result.events[0].fields.token, '18446744073709551615')
  assert.equal(result.events[0].raw, raw)
  assert.equal(result.issues[0].line, 2)
  assert.equal(result.missing, false)
  assert.equal((await (await fetch(base + 'no-events/events')).json()).missing, true)
  assert.equal((await fetch(base + 'missing/events')).status, 404)
})

test('运行列表可包含内置样例，报告接口仅公开指定文件', async t => {
  const f = await fixture(t)
  const name = 'verify-availability-1794158675872'
  await f.writeRun(name, manifest, partial)
  const app = await createDashboardServer({ resultsRoot: f.root })
  await new Promise(resolve => app.server.listen(0, '127.0.0.1', resolve))
  t.after(() => app.close())
  const base = 'http://127.0.0.1:' + app.server.address().port
  const list = async suffix => (await (await fetch(base + '/api/runs' + suffix)).json()).runs
  assert.deepEqual((await list('')).map(run => run.id), [name])
  const demo = (await list('?include=demo')).find(run => run.source === 'demo' && run.name === name)
  assert.ok(demo)
  const demoRoute = base + '/api/runs/' + encodeURIComponent(demo.id)
  assert.equal((await (await fetch(demoRoute)).json()).summary.success, true)
  assert.equal((await (await fetch(base + '/api/runs/' + name)).json()).status, 'failed')
  const log = await (await fetch(demoRoute + '/events')).json()
  assert.ok(log.events.some(event => event.event === 'failover_read_recovered'))
  const source = await fetch(base + '/api/evidence/latency-medians.tsv')
  assert.match(await source.text(), /achieved_rate\t/)
  assert.equal((await fetch(base + '/api/evidence/package.json')).status, 404)
})
