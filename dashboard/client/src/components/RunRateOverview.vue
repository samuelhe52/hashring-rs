<script setup lang="ts">
import { computed } from 'vue'
import DataChart from './DataChart.vue'
import RunFacts from './RunFacts.vue'
import RunReadings from './RunReadings.vue'
import { formatNumber as fmt, number, object, operations } from '../data'
import { hasLatencyCurve, latencyCurveOption, loadLabels, nodeLoadOption, nodeLoadRows, nodeLoadValue } from '../loadCharts'
import type { Measurement, RunDetail } from '../types'

const props = defineProps<{ run: RunDetail; rows: [string, Measurement][] }>()
const emit = defineEmits<{ details: [panel: 'latency' | 'nodes'] }>()
const stage = defineModel<string>('stage', { required: true })
const loadMetric = defineModel<string>('loadMetric', { required: true })
const operation = defineModel<string>('operation', { required: true })
const selected = computed(() => props.rows.find(([name]) => name === stage.value) ?? props.rows[0])
const current = computed(() => selected.value?.[1])
const currentLatency = computed(() => current.value?.latency_us?.[operation.value])
const curve = computed(() => latencyCurveOption(props.rows, operation.value, number(current.value?.achieved_rate)))
const nodes = computed(() => nodeLoadRows(current.value))
const nodeBars = computed(() => nodeLoadOption(nodes.value, loadMetric.value))
const counts = computed(() => Object.values(object(current.value?.errors)).filter(value => number(value) !== null).reduce<number>((sum, count) => sum + Number(count), 0))
const readings = computed(() => [
  {
    label: '实际成功速率', value: reading(current.value?.achieved_rate), unit: 'ops/s', tone: 'ok',
    note: '目标 ' + fmt(current.value?.target_rate) + ' ops/s · 正式 ' + fmt(current.value?.seconds, 2) + ' s',
  },
  {
    label: operation.value.toUpperCase() + ' p50', value: reading(currentLatency.value?.p50), unit: 'µs', tone: 'neutral',
    note: '正式阶段中位延迟',
  },
  {
    label: operation.value.toUpperCase() + ' p99', value: reading(currentLatency.value?.p99), unit: 'µs', tone: 'change',
    note: number(currentLatency.value?.samples) === null ? '样本数未记录' : fmt(currentLatency.value?.samples) + ' 个样本 · 不含预热',
  },
])
const balance = computed(() => {
  const data = current.value?.load_balance
  if (loadMetric.value === 'owned_keys' && number(data?.owned_keys_max_over_mean) !== null) return '键数 max/mean ' + fmt(data?.owned_keys_max_over_mean, 3)
  if (loadMetric.value === 'ops_handled' && number(data?.ops_handled_max_over_mean) !== null) return '操作数 max/mean ' + fmt(data?.ops_handled_max_over_mean, 3)
  return nodes.value.length ? nodes.value.length + ' 个节点有记录' : '本阶段未采集节点数据'
})
function reading(value: unknown) { return number(value) === null ? '未记录' : fmt(value, 2) }
</script>

<template>
  <section class="dashboard-content" aria-label="负载实验概览">
    <RunFacts :config="object(run.manifest?.config)">
      <label class="dashboard-stage-select">负载档位
        <select :value="selected?.[0]" @change="stage = ($event.target as HTMLSelectElement).value">
          <option v-for="[name, result] in rows" :key="name" :value="name">{{ fmt(result.target_rate) }} ops/s · {{ name }}</option>
        </select>
      </label>
    </RunFacts>
    <RunReadings :items="readings" label="当前负载档位读数" />
    <div v-if="current?.sender_saturated || counts > 0" class="notice warning dashboard-stage-warning" role="status">
      <p v-if="current?.sender_saturated">发送端未跟上计划。本阶段的实际速率为 {{ fmt(current.achieved_rate, 2) }} ops/s。</p>
      <p v-if="counts > 0">本阶段记录 {{ fmt(counts) }} 次错误。</p>
    </div>
    <div class="dashboard-pair">
      <section class="dashboard-card dashboard-curve-card">
        <header class="dashboard-card-heading">
          <h3>延迟—负载曲线</h3>
          <nav class="dashboard-card-tabs" aria-label="曲线操作类型">
            <button v-for="op in operations" :key="op" :aria-pressed="operation === op" @click="operation = op">{{ op.toUpperCase() }}</button>
          </nav>
        </header>
        <div class="dashboard-chart-body">
          <DataChart v-if="hasLatencyCurve(rows, operation)" :option="curve" :label="operation.toUpperCase() + ' p50、p99 延迟与实际速率曲线'" />
          <p v-else class="empty-inline">本场未记录所选操作的负载曲线。</p>
        </div>
        <footer class="dashboard-card-footer">
          <span>虚线标记当前档位</span><button class="table-link" aria-haspopup="dialog" @click="emit('details', 'latency')">延迟明细与导出 ↗</button>
        </footer>
      </section>
      <section class="dashboard-card dashboard-load-card">
        <header class="dashboard-card-heading">
          <h3>节点负载</h3>
          <select v-model="loadMetric" aria-label="节点负载指标">
            <option v-for="(label, metric) in loadLabels" :key="metric" :value="metric">{{ label }}</option>
          </select>
        </header>
        <div class="dashboard-chart-body dashboard-node-chart" :style="{ '--node-chart-min': (nodes.length * 22 + 43) + 'px' }">
          <DataChart v-if="nodes.some(([, node]) => nodeLoadValue(node, loadMetric) !== null)" :option="nodeBars" animate-updates :label="loadLabels[loadMetric] + '节点分布柱状图'" />
          <p v-else class="empty-inline">本阶段未采集所选节点指标。</p>
        </div>
        <footer class="dashboard-card-footer">
          <span>{{ balance }}</span><button class="table-link" aria-haspopup="dialog" @click="emit('details', 'nodes')">节点明细与导出 ↗</button>
        </footer>
      </section>
    </div>
  </section>
</template>
