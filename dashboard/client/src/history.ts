import type { Measurement, RunEvent } from './types'
import { latencyStatistic, number, object } from './data.ts'

export function replayBounds(events: RunEvent[], manifestStart?: number) {
  const origin = events.find(event => event.event === 'experiment_started')?.unix_ms ?? number(manifestStart) ?? events[0]?.unix_ms ?? 0
  const start = Math.min(origin, events[0]?.unix_ms ?? origin)
  return { start, end: Math.max(start, events.at(-1)?.unix_ms ?? start), origin }
}

// 同毫秒按原文件行号前进；拖动时间时则包含该毫秒的全部记录。
export function visibleCount(events: RunEvent[], cursor: number, line = Infinity): number {
  let low = 0
  let high = events.length
  while (low < high) {
    const mid = (low + high) >>> 1
    const event = events[mid]!
    if (event.unix_ms < cursor || (event.unix_ms === cursor && event.line <= line)) low = mid + 1
    else high = mid
  }
  return low
}

export function sampledSeries(events: RunEvent[], start: number, value: (event: RunEvent) => number | null, hideWarmup = false): [number, number | null][] {
  const rows: [number, number | null][] = []
  let previous: RunEvent | undefined
  for (const event of events) {
    if (event.event !== 'metrics_sampled' || (hideWarmup && event.fields.warmup === true)) continue
    const window = number(event.fields.window_ms)
    const phase = event.fields.stage ?? event.fields.phase
    if (previous && (phase !== (previous.fields.stage ?? previous.fields.phase) || window === null || window <= 0 || event.unix_ms - previous.unix_ms > window * 1.5)) {
      rows.push([((previous.unix_ms + event.unix_ms) / 2 - start) / 1000, null])
    }
    rows.push([(event.unix_ms - start) / 1000, value(event)])
    previous = event
  }
  return rows
}

export function warmupIntervals(events: RunEvent[], start: number): [number, number][] {
  const intervals: [number, number][] = []
  for (const event of events) {
    const window = number(event.fields.window_ms)
    if (event.event !== 'metrics_sampled' || event.fields.warmup !== true || window === null || window <= 0) continue
    const from = Math.max(start, event.unix_ms - window)
    const last = intervals.at(-1)
    if (last && from <= last[1]) last[1] = Math.max(last[1], event.unix_ms)
    else intervals.push([from, event.unix_ms])
  }
  return intervals.map(([from, to]) => [(from - start) / 1000, (to - start) / 1000])
}

export function windowLatency(event: RunEvent, operation: string, metric: string): number | null {
  const direct = object(object(event.fields.latency_us)[operation])
  const metrics = object(event.fields.metrics)
  const cohort = object(metrics[operation])
  // scaling can report several phase buckets in one window. Use the recorded phase
  // for a generic GET/PUT series and keep explicitly requested phase buckets intact.
  const phase = typeof event.fields.phase === 'string' ? object(metrics[event.fields.phase + '|' + operation]) : {}
  return latencyStatistic(direct, metric) ?? latencyStatistic(cohort.latency_us, metric) ?? latencyStatistic(phase.latency_us, metric)
}

// latency 的 ops 是该窗口已完成且成功的操作，不把 issued 当成成功。
export function windowSuccessRate(event: RunEvent): number | null {
  const window = number(event.fields.window_ms)
  const ops = Object.values(object(event.fields.ops))
  if (window === null || window <= 0 || !ops.length || ops.some(value => number(value) === null)) return null
  return ops.reduce<number>((total, value) => total + Number(value), 0) * 1000 / window
}

export function scalingIssueRate(event: RunEvent): number | null {
  const window = number(event.fields.window_ms)
  return event.fields.mode === 'scaling' && window !== null && window > 0
    ? number(event.fields.achieved_rate)
    : null
}

export function nodeOperationSample(event: RunEvent, stage?: Measurement) {
  const average = !Object.hasOwn(event.fields, 'node_ops_per_second')
  const seconds = number(stage?.seconds) ?? 0
  if (average && (event.fields.warmup === true || seconds <= 0)) return null
  const source = average
    ? Object.entries(stage?.load_balance?.per_node ?? {}).map(([id, node]) => [id, node?.ops_handled] as const)
    : Object.entries(object(event.fields.node_ops_per_second))
  const values = new Map<string, number>()
  for (const [id, raw] of source) {
    const value = number(raw)
    if (value !== null && value >= 0) values.set(id, average ? value / seconds : value)
  }
  return values.size ? { values, average } : null
}
