import assert from 'node:assert/strict'
import { setTimeout as delay } from 'node:timers/promises'

const base = 'http://127.0.0.1:4175'
const stamp = String(Date.now())
async function request(path, data) {
  const response = await fetch(base + path, data === undefined ? {} : {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data),
  })
  return { status: response.status, body: await response.json() }
}
async function waitFor(check, timeout = 90000) {
  const end = Date.now() + timeout
  while (Date.now() < end) {
    const found = await check()
    if (found) return found
    await delay(350)
  }
  throw new Error('等待实验条件超时')
}
const abort = new AbortController()
const frames = []
const read = (async () => {
  const response = await fetch(base + '/api/experiments/current/stream', { signal: abort.signal })
  assert.match(response.headers.get('content-type'), /text\/event-stream/)
  let pending = ''
  for await (const text of response.body.pipeThrough(new TextDecoderStream())) {
    pending += text
    let boundary
    while ((boundary = pending.indexOf('\n\n')) >= 0) {
      const frame = pending.slice(0, boundary)
      pending = pending.slice(boundary + 2)
      const data = frame.split('\n').find(line => line.startsWith('data: '))
      if (data) frames.push(JSON.parse(data.slice(6)))
    }
  }
})().catch(error => { if (!abort.signal.aborted) throw error })

const start = async data => {
  const preview = await request('/api/experiments/preview', data)
  assert.equal(preview.status, 200, JSON.stringify(preview.body))
  assert.match(preview.body.script, /^#!\/usr\/bin\/env bash/)
  const result = await request('/api/experiments', data)
  assert.equal(result.status, 201, JSON.stringify(result.body))
  return result.body.id
}
const finish = id => waitFor(async () => {
  const { body } = await request('/api/experiments/current')
  return body.session?.id === id && body.session.endedAt && body.session
})
const records = id => frames.filter(frame => frame.session?.id === id).flatMap(frame => frame.events ?? frame.log?.events ?? [])
const latency = { kind: 'form', name: 'verify-latency-' + stamp, mode: 'latency', nodes: 3, keys: 2000, rf: 3, concurrency: 16, valueBytes: 128, seed: '1', virtualNodes: 128, policy: 'owner-only', rates: '150,300', warmup: 1, duration: 5, mix: '90:9:1', distribution: 'uniform', maxInFlight: 4096 }
try {
  await waitFor(() => frames.length)
  const first = await start(latency)
  assert.equal((await request('/api/experiments', { ...latency, name: first + '-parallel' })).status, 409)
  await waitFor(() => records(first).some(event => event.event === 'metrics_sampled'))
  const topology = await waitFor(async () => {
    const result = await request('/api/experiments/current/topology?id=' + first)
    return result.status === 200 && result.body
  }, 12000)
  assert.equal(topology.ring.members.length, 3)
  assert.equal(topology.replicas.nodes.length, 3)
  const done = await finish(first)
  assert.equal(done.phase, 'succeeded', JSON.stringify(done))
  const result = await request('/api/runs/' + first)
  assert.equal(result.body.status, 'success')
  assert.equal(Object.keys(result.body.summary.measurements).length, 2)
  console.log('latency:', first, records(first).filter(event => event.event === 'metrics_sampled').length, 'SSE samples, topology and replica status OK')

  const interrupted = await start({ ...latency, name: 'verify-stop-' + stamp, duration: 20 })
  await waitFor(() => records(interrupted).some(event => event.event === 'topology_published'))
  assert.equal((await request('/api/experiments/current/stop', { id: first })).status, 409)
  assert.equal((await request('/api/experiments/current/stop', { id: interrupted })).status, 200)
  const stopped = await finish(interrupted)
  assert.equal(stopped.phase, 'stopped', JSON.stringify(stopped))
  assert.equal(stopped.hasResult, true)
  assert.equal((await request('/api/runs/' + interrupted)).body.status, 'incomplete')
  console.log('stop:', interrupted, 'partial manifest and events retained')

  for (const [mode, keys, expected] of [['correctness', 2000, 'migration_complete'], ['availability', 300, 'failover_published']]) {
    const id = await start({ kind: 'yaml', name: 'verify-' + mode + '-' + stamp, yaml: `mode: ${mode}\nnodes: 4\nkeys: ${keys}\nvirtual_nodes: 32\nconcurrency: 8\nworker_threads: 2\ncoordinator_worker_threads: 2\n` })
    const complete = await finish(id)
    const data = await request('/api/runs/' + id + '/events')
    assert.equal(complete.phase, 'succeeded', JSON.stringify(complete))
    assert.ok(data.body.events.some(event => event.event === expected), 'missing ' + expected)
    const epochs = [...new Set(data.body.events.filter(event => event.event === 'topology_published').map(event => event.fields.epoch))]
    assert.ok(epochs.length >= 2)
    console.log(mode + ':', id, expected, 'epochs', epochs.join(' → '))
  }
  assert.equal((await request('/api/experiments', latency)).status, 409)
  console.log('duplicate name and concurrent launch both rejected')
} finally {
  const { body } = await request('/api/experiments/current')
  if (body.session && !body.session.endedAt) await request('/api/experiments/current/stop', { id: body.session.id })
  abort.abort()
  await read
}
