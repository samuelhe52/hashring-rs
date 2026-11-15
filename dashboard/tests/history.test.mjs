import test from 'node:test'
import assert from 'node:assert/strict'
import { replayBounds, visibleCount, sampledSeries, warmupIntervals, windowLatency } from '../client/src/history.ts'

const e = (line, unix_ms, fields = {}, event = 'metrics_sampled') => ({ line, unix_ms, fields, event, raw: '{}' })
test('回放不读未来，同毫秒按行前进，起点优先实验启动', () => {
  const events = [e(1, 1000, {}, 'experiment_started'), e(2, 2000), e(3, 2000), e(4, 4000)]
  assert.deepEqual(replayBounds(events, 500), { start: 1000, end: 4000, origin: 1000 })
  assert.equal(visibleCount(events, 1999), 1)
  assert.equal(visibleCount(events, 2000, 2), 2)
  assert.equal(visibleCount(events, 2000), 3)
  assert.equal(visibleCount(events, 9999), 4)
  assert.deepEqual(replayBounds([], 500), { start: 500, end: 500, origin: 500 })
})
test('缺窗口和阶段切换断线，保留零值，不插值或平均 p99', () => {
  const events = [e(1, 1000, { stage: 'a', window_ms: 1000, value: 0 }), e(2, 5000, { stage: 'a', window_ms: 1000, value: 732 }), e(3, 6000, { stage: 'b', window_ms: 1000, value: 660 })]
  assert.deepEqual(sampledSeries(events, 0, e => e.fields.value), [[1, 0], [3, null], [5, 732], [5.5, null], [6, 660]])
  assert.equal(windowLatency(e(1, 1000, { latency_us: { get: { p99: 0 } } }), 'get', 'p99'), 0)
  assert.equal(windowLatency(e(1, 1000), 'get', 'p99'), null)
  assert.deepEqual(sampledSeries([e(1, 1000, { value: 1 }), e(2, 2000, { value: 2 })], 0, e => e.fields.value), [[1, 1], [1.5, null], [2, 2]])
})
test('预热按记录的窗口区间着色，缺窗口宽度不猜；可隐藏预热', () => {
  const events = [e(1, 1000, { warmup: true, window_ms: 1000 }), e(2, 2000, { warmup: true, window_ms: 1000 }), e(3, 3000, { warmup: true }), e(4, 4000, { warmup: false, value: 4 })]
  assert.deepEqual(warmupIntervals(events, 0), [[0, 2]])
  assert.deepEqual(sampledSeries(events, 0, e => e.fields.value ?? null, true), [[4, 4]])
})

test('扩容窗口的多阶段样本按记录中的阶段读取分位数', () => {
  const sample = e(1, 1000, { phase: 'post', metrics: {
    'published|get': { latency_us: { p99: 920 } },
    'post|get': { latency_us: { p99: 440 } },
    'published|put_new': { latency_us: { p99: 680 } },
  } })
  assert.equal(windowLatency(sample, 'get', 'p99'), 440)
  assert.equal(windowLatency(sample, 'published|get', 'p99'), 920)
  assert.equal(windowLatency(sample, 'put_new', 'p99'), null)
})
