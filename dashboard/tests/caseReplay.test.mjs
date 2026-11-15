import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { parseEventLog } from '../server/events.mjs'
import { casePlayback, headline, latestPublishedEpoch, stageDetails } from '../client/src/caseReplay.ts'
import { visibleCount } from '../client/src/history.ts'

async function sample(id) {
  const base = new URL('../demo/' + id + '/', import.meta.url)
  const [events, summary] = await Promise.all(['events.jsonl', 'summary.json'].map(file => readFile(new URL(file, base), 'utf8')))
  return { events: parseEventLog(events).events, summary: JSON.parse(summary) }
}

test('故障回放区分发布、读恢复和完整副本，阶段只引用到达的 epoch', async () => {
  const { events, summary } = await sample('verify-availability-1794158675872')
  const replay = casePlayback('fault', events)
  assert.deepEqual(replay.stages.map(stage => stage.kind), ['killed', 'publish', 'read', 'restored'])
  assert.deepEqual(replay.stages.map(stage => stageDetails(stage, replay, summary).elapsed.toFixed(2)), ['0.00', '5.13', '5.19', '10.30'])
  assert.deepEqual(headline('fault', replay, summary).map(reading => reading.value), ['5.19', '10.30', '128 / 128'])
  const publish = replay.stages[1].event
  const seen = events.slice(0, visibleCount(events, publish.unix_ms, publish.line))
  assert.equal(latestPublishedEpoch(seen), '3')
  assert.equal(seen.filter(event => event.event === 'topology_published').at(-1).fields.epoch, 2)
  const details = stageDetails(replay.stages.at(-1), replay, summary)
  assert.equal(details.facts.find(fact => fact.label === '写探针自身耗时').source.event, 'write_after_failover')
})

test('扩容回放只采用 scale_out 校验，完整记录包含缩容后的快照', async () => {
  const { events, summary } = await sample('verify-correctness-1794158675872')
  const replay = casePlayback('expansion', events)
  assert.deepEqual(replay.stages.map(stage => stage.kind), ['before', 'grow', 'copy', 'migrated', 'verified'])
  assert.deepEqual(headline('expansion', replay, summary).map(reading => reading.value), ['2,000', '172', '171'])
  assert.equal(latestPublishedEpoch(replay.records), '1')
  assert.equal(latestPublishedEpoch(events), '3')
  assert.equal(replay.records.some(event => event.fields.label === 'scale_in'), false)
})

test('同毫秒的扩容快照按原行到达，零违规保留，未记录的读数保持缺失', () => {
  const topology = (epoch, nodes) => ({
    epoch, desired_replication_factor: 1, members: nodes.map(node_id => ({ node_id })),
    assignments: nodes.map((node_id, index) => ({ node_id, token: index })),
  })
  const data = [
    ['topology_published', 1, topology(1, ['node-1'])],
    ['scaling_transition_started', 2, { label: 'grow', base_epoch: 1, target_epoch: 2, keys_moved: 0 }],
    ['scaling_cohorts_verified', 3, { label: 'grow', keys: 0, violations: 0 }],
    ['scaling_transition_complete', 3, { label: 'grow', target_epoch: 2 }],
    ['topology_published', 3, topology(2, ['node-1', 'node-2'])],
  ]
  const { events } = parseEventLog(data.map(([event, unix_ms, fields]) => JSON.stringify({ event, unix_ms, fields })).join('\n'))
  const replay = casePlayback('expansion', events)
  const cut = visibleCount(replay.records, 3, 3)
  assert.equal(latestPublishedEpoch(replay.records.slice(0, cut)), '1')
  assert.equal(replay.stages.at(-1).kind, 'snapshot')
  assert.equal(latestPublishedEpoch(replay.records), '2')
  assert.deepEqual(headline('expansion', replay).map(reading => reading.value), ['0', '0', '0'])
  assert.deepEqual(headline('fault', casePlayback('fault', [])).map(reading => reading.value), ['未记录', '未记录', '未记录'])
})
