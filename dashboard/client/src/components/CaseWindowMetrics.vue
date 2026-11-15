<script setup lang="ts">
import { computed, ref } from 'vue'
import DataChart from './DataChart.vue'
import { colors, formatNumber as fmt, number, object } from '../data'
import { sampledSeries, warmupIntervals, windowLatency } from '../history.ts'
import type { RunEvent } from '../types'

const props = defineProps<{ seen: RunEvent[]; records: RunEvent[]; marks: RunEvent[]; start: number; end: number; origin: number; cursor: number; axisLabel: string }>()
const plot = ref<'latency' | 'counts'>('latency')
const chart = ref<InstanceType<typeof DataChart> | null>(null)
const layout = ref(0)
const countError = (event: RunEvent) => {
  const errors = event.fields.errors
  if (!errors || typeof errors !== 'object' || Array.isArray(errors)) return null
  const counts = Object.values(object(errors)).map(number)
  return counts.every(count => count !== null) ? counts.reduce<number>((sum, count) => sum + (count ?? 0), 0) : null
}
const metricSamples = computed(() => props.records.filter(event => event.event === 'metrics_sampled'))
const latencySeries = [
  { name: 'GET p99', operation: 'get' }, { name: 'PUT p99', operation: 'put' },
  { name: 'PUT 改写 p99', operation: 'put_rewrite' }, { name: 'PUT 新写 p99', operation: 'put_new' },
].map(({ name, operation }) => ({ name, read: (event: RunEvent) => windowLatency(event, operation, 'p99') }))
const countSeries = [
  { name: '重试', read: (event: RunEvent) => number(event.fields.retries) },
  { name: '错误', read: countError },
  { name: '未按时发出', read: (event: RunEvent) => number(event.fields.late) },
]
const definitions = computed(() => (plot.value === 'latency' ? latencySeries : countSeries)
  .filter(series => metricSamples.value.some(event => series.read(event) !== null)))
const unit = computed(() => plot.value === 'latency' ? 'µs' : '次 / 窗口')
const elapsed = (time: number) => (time - props.origin) / 1000
const playhead = computed(() => { void layout.value; return chart.value?.pixelAt(elapsed(props.cursor)) ?? null })
const options = computed(() => ({
  color: [colors[1], colors[0], colors[3], colors[4]],
  legend: { bottom: 3, itemWidth: 12, itemHeight: 8, textStyle: { fontSize: 11 } },
  grid: { top: 26, right: 28, bottom: 57, left: 56, outerBoundsMode: 'none' },
  tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => fmt(Array.isArray(value) ? value.at(-1) : value, 2) + ' ' + unit.value },
  xAxis: { type: 'value', min: Math.floor(elapsed(props.start)), max: Math.max(1, Math.ceil(elapsed(props.end))), splitNumber: 5,
    name: props.axisLabel, nameLocation: 'middle', nameGap: 22, axisLabel: { hideOverlap: true } },
  yAxis: { type: 'value', min: 0, name: unit.value, splitNumber: 3, axisLabel: { hideOverlap: true }, splitLine: { lineStyle: { color: '#e5e0d7', type: 'dashed' } } },
  series: definitions.value.map((series, index) => ({
    name: series.name, type: 'line', symbolSize: 5, connectNulls: false,
    lineStyle: { width: 2, type: index % 2 === 0 ? 'solid' : 'dashed' },
    data: sampledSeries(props.seen, props.origin, series.read),
    ...(index === 0 ? {
      markArea: { silent: true, itemStyle: { color: '#ccc6bb40' },
        data: warmupIntervals(props.seen, props.start).map(([from, to]) => [{ xAxis: from + elapsed(props.start) }, { xAxis: to + elapsed(props.start) }]) },
      markLine: { silent: true, symbol: ['none', 'none'], label: { show: false }, lineStyle: { color: '#a84924', opacity: .42, type: 'dashed' },
        data: props.marks.map(event => ({ xAxis: elapsed(event.unix_ms) })) },
    } : {}),
  })),
}))
const hasVisibleValue = computed(() => definitions.value.some(series => props.seen.some(event => event.event === 'metrics_sampled' && series.read(event) !== null)))
</script>

<template>
  <div class="case-window-metrics">
    <div class="case-metric-options segmented" aria-label="窗口指标">
      <button :aria-pressed="plot === 'latency'" @click="plot = 'latency'">窗口 p99</button>
      <button :aria-pressed="plot === 'counts'" @click="plot = 'counts'">计数</button>
    </div>
    <div v-if="definitions.length" class="replay-chart">
      <DataChart ref="chart" :option="options" :label="plot === 'latency' ? '变更期间 GET / PUT p99（µs）' : '变更期间窗口计数'" :motion="false" @layout="layout++" />
      <div v-if="playhead !== null" class="chart-playhead" :style="{ transform: 'translateX(' + playhead + 'px)' }" aria-hidden="true"></div>
      <p v-if="!hasVisibleValue" class="chart-empty-label">等待回放到有记录的采样窗口</p>
    </div>
    <p v-else class="empty-inline">本场未记录所选窗口指标。</p>
  </div>
</template>
