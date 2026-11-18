import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { createStore } from './store.mjs'
import { fixture, manifest, partial } from './test-fixture.mjs'

test('发现嵌套运行，按时间排列；保留失败的部分指标和真实零值', async t => {
  const f = await fixture(t)
  await f.writeRun('campaign/run', manifest, partial)
  await f.writeRun('newer', { ...manifest, started_unix_ms: manifest.started_unix_ms + 1 }, { success: true })
  const store = createStore(f.root)
  assert.deepEqual((await store.listRuns()).map(run => run.id), ['newer', 'campaign/run'])
  const run = await store.readRun('campaign/run')
  assert.equal(run.status, 'failed')
  assert.equal(run.summary.error, 'verification failed')
  assert.equal(run.summary.measurements.rate_0.latency_us.get.p99, 732)
  assert.equal(run.summary.measurements.rate_0.retries, 0)
  assert.equal(run.manifest.source_reproducible, false)
  assert.equal('runtime_source' in run.manifest, false)
})

test('轻量元数据可读取运行并保留完整清单下载', async t => {
  const f = await fixture(t)
  const directory = await f.writeRun('indexed', undefined, { success: true, elapsed_seconds: 12 })
  const { runtime_source, ...metadata } = manifest
  await writeFile(path.join(directory, 'run-metadata.json'), JSON.stringify(metadata))
  const store = createStore(f.root)
  const [listed] = await store.listRuns()
  assert.equal(listed.status, 'success')
  assert.equal(listed.started_unix_ms, manifest.started_unix_ms)
  assert.equal(listed.elapsed_seconds, 12)
  assert.deepEqual(listed.config, manifest.config)
  assert.deepEqual((await store.readRun('indexed')).manifest.config, manifest.config)

  const raw = JSON.stringify({ ...metadata, runtime_source })
  await writeFile(path.join(directory, 'manifest.json'), raw)
  assert.equal(await store.rawFile('indexed', 'manifest.json'), raw)
  await writeFile(path.join(directory, 'run-metadata.json'), '{')
  assert.deepEqual((await store.readRun('indexed')).manifest.config, manifest.config)
})

test('刷新时重新读取发生变化的完整清单与运行状态', async t => {
  const f = await fixture(t)
  const directory = await f.writeRun('changing', manifest)
  const store = createStore(f.root)
  const [initial] = await store.listRuns()
  assert.equal(initial.status, 'incomplete')
  assert.equal(initial.config.key_count, manifest.config.key_count)

  const config = { ...manifest.config, key_count: 50_000 }
  await writeFile(path.join(directory, 'manifest.json'), JSON.stringify({ ...manifest, config }))
  await writeFile(path.join(directory, 'summary.json'), JSON.stringify({ success: true }))
  const [updated] = await store.listRuns()
  assert.equal(updated.status, 'success')
  assert.deepEqual(updated.config, config)
})

test('结构化模式记录的 u64 不舍入，零值与 null 保留', async t => {
  const f = await fixture(t)
  const dir = await f.writeRun('wide', manifest)
  await writeFile(path.join(dir, 'summary.json'), '{"success":false,"observations":{"fault_rounds":[{"token":18446744073709551615,"recover_seconds":0,"min_live_rf":null}]}}')
  const record = (await createStore(f.root).readRun('wide')).summary.observations.fault_rounds[0]
  assert.equal(record.token, '18446744073709551615')
  assert.equal(record.recover_seconds, 0)
  assert.equal(record.min_live_rf, null)
})

test('没有 summary 和损坏 JSON 都保留为未完成；不把缺字段伪造成零', async t => {
  const f = await fixture(t)
  await f.writeRun('unfinished', manifest)
  const broken = await f.writeRun('broken', manifest)
  await writeFile(path.join(broken, 'summary.json'), '{broken')
  const store = createStore(f.root)
  const unfinished = await store.readRun('unfinished')
  assert.equal(unfinished.status, 'incomplete')
  assert.equal(unfinished.summary, null)
  assert.equal(unfinished.summary?.elapsed_seconds, undefined)
  const run = await store.readRun('broken')
  assert.equal(run.status, 'incomplete')
  assert.match(run.warnings[0], /summary.json/)
  assert.equal((await store.listRuns()).length, 2)
})

test('原文逐字保留 u64；只允许结果文件和日志，拒绝路径越界与链接逃逸', async t => {
  const f = await fixture(t)
  const dir = await f.writeRun('run', manifest, partial)
  const raw = '{"event":"topology_published","fields":{"token":18446744073709551615}}\n'
  await writeFile(path.join(dir, 'events.jsonl'), raw)
  await mkdir(path.join(dir, 'process-logs'))
  await writeFile(path.join(dir, 'process-logs/node-1.stderr.log'), 'diagnostic\n')
  const store = createStore(f.root)
  assert.equal(await store.rawFile('run', 'events.jsonl'), raw)
  assert.deepEqual(await store.logs('run'), ['node-1.stderr.log'])
  assert.equal(await store.logs('run', 'node-1.stderr.log'), 'diagnostic\n')
  await assert.rejects(store.rawFile('run', 'coordinator.redb'), { status: 400 })
  await assert.rejects(store.readRun('..'), { status: 400 })
  await assert.rejects(store.logs('run', '../node-1.stderr.log'), { status: 400 })
  const outside = path.join(f.temp, 'outside.txt')
  await writeFile(outside, 'outside data')
  await symlink(outside, path.join(dir, 'process-logs/escape.log'))
  await assert.rejects(store.logs('run', 'escape.log'), { status: 400 })
  await assert.rejects(store.rawFile('run', '../../outside.txt'), { status: 400 })
  assert.equal(await readFile(outside, 'utf8'), 'outside data')
})

test('结果根目录尚不存在时返回空列表，未采集 CPU 保持 null', async t => {
  const f = await fixture(t)
  assert.deepEqual(await createStore(path.join(f.temp, 'not-created')).listRuns(), [])
  await f.writeRun('null-cpu', manifest, { success: true, measurements: { rate_0: { target_rate: 500, load_balance: { per_node: { 'node-1': { cpu_percent: null } } } } } })
  const run = await createStore(f.root).readRun('null-cpu')
  assert.equal(run.summary.measurements.rate_0.load_balance.per_node['node-1'].cpu_percent, null)
})
