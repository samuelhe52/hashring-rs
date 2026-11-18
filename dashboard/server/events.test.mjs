import test from 'node:test'
import assert from 'node:assert/strict'
import { symlink, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { parseEventLog } from './events.mjs'
import { createStore } from './store.mjs'
import { fixture, manifest } from './test-fixture.mjs'

test('JSONL 按时间稳定排序，保留原始行号、空行位置和相同时间的重复事件', () => {
  const a = '{"event":"second","unix_ms":2000,"fields":{}}'
  const b = '{"event":"first","unix_ms":1000,"fields":{}}'
  const result = parseEventLog(`\r\n${a}\r\n${b}\r\n${b}`)
  assert.deepEqual(result.events.map(e => [e.event, e.line]), [['first', 3], ['first', 4], ['second', 2]])
  assert.equal(result.events[0].raw, b)
  assert.deepEqual(result.issues, [])
})

test('拓扑 u64 按原始数字文本转换为字符串，普通数值和字符串不受影响', () => {
  const raw = '{"event":"topology_published","unix_ms":1793122444410,"fields":{"epoch":1,"assignments":[{"token":18446744073709551615},{"token":9007199254740993}],"offset":-9007199254740993,"safe":9007199254740991,"seconds":0.125,"name":"token: 18446744073709551615"}}'
  const { events, issues } = parseEventLog(raw)
  assert.deepEqual(issues, [])
  const event = events[0]
  assert.deepEqual(event.fields.assignments.map(a => a.token), ['18446744073709551615', '9007199254740993'])
  assert.equal(event.fields.offset, '-9007199254740993')
  assert.equal(event.fields.safe, 9007199254740991)
  assert.equal(event.fields.seconds, 0.125)
  assert.equal(event.raw, raw)
  assert.equal(JSON.parse(JSON.stringify(event)).fields.assignments[0].token, '18446744073709551615')
})

test('损坏、无效形状与未写完的尾行明确报告，正常行和未知事件继续保留', () => {
  const good = '{"event":"future_event","unix_ms":0,"fields":{"warmup":false,"issued":0}}'
  const result = parseEventLog([
    '{bad', good, '{"event":"no_time","fields":{}}',
    '{"event":"bad_fields","unix_ms":10,"fields":[]}',
    '{"event":"unsafe_time","unix_ms":9007199254740993,"fields":{}}',
    '{"event":"partial"',
  ].join('\n'))
  assert.equal(result.events.length, 1)
  assert.equal(result.events[0].event, 'future_event')
  assert.equal(result.events[0].fields.issued, 0)
  assert.equal(result.events[0].fields.warmup, false)
  assert.deepEqual(result.issues.map(e => e.line), [1, 3, 4, 5, 6])
  assert.equal(result.issues[4].raw, '{"event":"partial"')
  assert.deepEqual(parseEventLog(' \n\r\n'), { events: [], issues: [] })
})

test('事件缺失、空文件、读取边界分别处理；不写回源文件', async t => {
  const f = await fixture(t)
  const dir = await f.writeRun('campaign/run', manifest)
  const store = createStore(f.root)
  assert.deepEqual(await store.events('campaign/run'), { events: [], issues: [], missing: true })
  const file = path.join(dir, 'events.jsonl')
  await writeFile(file, '')
  assert.deepEqual(await store.events('campaign/run'), { events: [], issues: [], missing: false })
  await assert.rejects(store.events('missing'), { code: 'ENOENT' })
  await assert.rejects(store.events('..'), { status: 400 })
  const outside = path.join(f.temp, 'outside.jsonl')
  await writeFile(outside, '{"event":"outside","unix_ms":1,"fields":{}}')
  const other = await f.writeRun('linked', manifest)
  await symlink(outside, path.join(other, 'events.jsonl'))
  await assert.rejects(store.events('linked'), { status: 400 })
})
