import test from 'node:test'
import assert from 'node:assert/strict'
import { canonical, configRows, comparisonRates, stagesAtRate, numericFields } from '../client/src/compare.ts'

const run = (config, measurements) => ({ manifest: { config }, summary: { measurements } })
test('对比按目标速率匹配，缺阶段不回退，重复目标保留可选择的各阶段', () => {
  const a = run({}, { rate_0: { target_rate: 500 }, rate_1: { target_rate: 1000 } })
  const b = run({}, { rate_0: { target_rate: 1000 }, rate_1: { target_rate: 1000 }, rate_2: { target_rate: 2000 } })
  assert.deepEqual(comparisonRates([a, b]), [500, 1000, 2000])
  assert.equal(stagesAtRate(b, 500).length, 0)
  assert.deepEqual(stagesAtRate(b, 1000).map(([key]) => key), ['rate_0', 'rate_1'])
})
test('配置深比较不受字段顺序影响，缺字段与 null 区分', () => {
  assert.equal(canonical({ a: 0, b: { c: false } }), canonical({ b: { c: false }, a: 0 }))
  const rows = configRows([run({ rf: 3, option: null }), run({ rf: 3 })])
  assert.equal(rows.find(row => row.key === 'rf').different, false)
  assert.equal(rows.find(row => row.key === 'option').different, true)
})
test('保留 0 与独立 p99，不把字符串 u64、null 或坏数值当成绩', () => {
  assert.deepEqual([...numericFields({ rounds: [{ p99: 732 }, { p99: 660 }], zero: 0, missing: null, token: '18446744073709551615', bad: NaN })], [['rounds.0.p99', 732], ['rounds.1.p99', 660], ['zero', 0]])
})
