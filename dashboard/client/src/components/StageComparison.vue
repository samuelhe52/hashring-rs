<script setup lang="ts">
import { computed, ref } from 'vue'
import DataChart from './DataChart.vue'
import { colors, formatNumber as fmt, number, operations, stages } from '../data'
import type { RunDetail } from '../types'

const props = defineProps<{ run: RunDetail }>()
const metric = ref('p99')
const operation = ref('get')
const rows = computed(() => stages(props.run.summary))
const unit = computed(() => metric.value === 'rate' ? 'ops/s' : 'µs')
const title = computed(() => metric.value === 'rate' ? '各阶段实际成功速率（ops/s）' : `${operation.value.toUpperCase()} ${metric.value} 阶段对比（µs）`)
const values = computed(() => rows.value.map(([, stage]) => number(metric.value === 'rate' ? stage.achieved_rate : stage.latency_us?.[operation.value]?.p99)))
const first = computed(() => values.value[0] ?? null)
const last = computed(() => values.value.at(-1) ?? null)
const delta = computed(() => first.value !== null && last.value !== null ? last.value - first.value : null)
const option = computed(() => ({
  grid: { top: 15, left: 8, right: 55, bottom: 35, outerBoundsMode: 'same' },
  tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => `${fmt(value, 2)} ${unit.value}` },
  xAxis: { type: 'value', min: 0, name: unit.value, nameGap: 9, axisLabel: { hideOverlap: true }, splitLine: { lineStyle: { color: '#e5e0d7', type: 'dashed' } } },
  yAxis: { type: 'category', inverse: true, data: rows.value.map(([, value]) => fmt(value.target_rate) + ' ops/s'), axisLabel: { width: 106, overflow: 'truncate' }, axisTick: { show: false }, axisLine: { show: false } },
  series: [{ id: 'stage-comparison', type: 'bar', barMaxWidth: 24, showBackground: true, backgroundStyle: { color: '#f3f1ec' },
    label: { show: true, position: 'right', formatter: (item: { value: unknown }) => fmt(item.value, 2) },
    itemStyle: { color: metric.value === 'rate' ? colors[1] : colors[0], borderRadius: [0, 4, 4, 0] },
    data: values.value.map((value, index) => ({ name: rows.value[index]?.[0], value })),
  }],
}))
</script>

<template>
  <div class="stage-comparison" :class="{ 'showing-rate': metric === 'rate' }">
    <div class="comparison-controls">
      <div class="segmented" aria-label="阶段对比指标">
        <button :aria-pressed="metric === 'p99'" @click="metric = 'p99'">p99 延迟</button>
        <button :aria-pressed="metric === 'rate'" @click="metric = 'rate'">实际速率</button>
      </div>
      <select v-if="metric === 'p99'" v-model="operation" aria-label="阶段对比操作"><option v-for="op in operations" :key="op" :value="op">{{ op.toUpperCase() }}</option></select>
    </div>
    <div v-if="rows.length > 1 && delta !== null" :key="title" class="case-delta">
      <div><span>第一档</span><strong>{{ fmt(first, 2) }}</strong></div>
      <span class="case-delta-arrow" aria-hidden="true">→</span>
      <div><span>最后一档</span><strong>{{ fmt(last, 2) }}</strong></div>
      <div class="delta-value"><span>差值</span><strong>{{ delta > 0 ? '+' : '' }}{{ fmt(delta, 2) }} <small>{{ unit }}</small></strong></div>
    </div>
    <DataChart v-if="values.some(value => value !== null)" :option="option" :label="title" :source="run.id + ' / 目标速率分组'" :animate-updates="true" exportable />
    <p v-else class="empty-inline">这个指标没有记录。</p>
  </div>
</template>
