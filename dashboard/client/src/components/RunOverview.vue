<script setup lang="ts">
import { computed, ref } from 'vue'
import DataChart from './DataChart.vue'
import ModeResults from './ModeResults.vue'
import type { Latency, Measurement, RunDetail } from '../types'
import { colors, errorsText, fieldNames, formatNumber as fmt, latencyRows, number, object, operations, stages, text, throughputRows } from '../data'
import { hasLatencyCurve, latencyCurveOption, loadLabels, nodeLoadOption, nodeLoadRows, nodeLoadValue } from '../loadCharts'

const props = defineProps<{ run: RunDetail; section: 'latency' | 'nodes' | 'observations'; stage: string }>()
const emit = defineEmits<{ selectStage: [name: string] }>()
const view = defineModel<string>('view', { default: 'curve' })
const observationView = defineModel<string>('observationView', { default: 'throughput' })
const curveOperation = defineModel<string>('operation', { default: 'get' })
const loadMetric = defineModel<string>('loadMetric', { default: 'owned_keys' })
const pressureColumns = [
  { key: 'dedup_bytes', label: '去重占用', unit: 'B' },
  { key: 'dedup_capacity_bytes', label: '去重容量', unit: 'B' },
  { key: 'owner_dedup_rejections', label: 'Owner 拒绝', unit: '次' },
  { key: 'follower_dedup_rejections', label: 'Follower 拒绝', unit: '次' },
  { key: 'replication_rpc_retries', label: '复制 RPC 重试', unit: '次' },
  { key: 'replication_stream_peak_pending', label: '队列峰值', unit: '条' },
] as const
const pressureMetric = ref<(typeof pressureColumns)[number]['key']>('dedup_bytes')
const percentiles = ['p50', 'p95', 'p99', 'max'] as const
const rateStages = computed(() => stages(props.run.summary))
const currentEntry = computed(() => rateStages.value.find(([name]) => name === props.stage) ?? rateStages.value[0])
const current = computed(() => currentEntry.value?.[1])
const observations = computed(() => object(props.run.summary?.observations))
const throughput = computed(() => throughputRows(props.run.summary))
const distribution = computed(() => latencyRows(current.value?.latency_us))
const nodeLoads = computed(() => nodeLoadRows(current.value))
const idleRows = computed(() => [
  { label: '客户端 PUT', stats: object(observations.value.idle_client_put_latency_us ?? observations.value.client_put_latency_us) },
  { label: '直连 Owner RPC PUT', stats: object(observations.value.idle_owner_rpc_put_latency_us ?? observations.value.owner_rpc_put_latency_us) },
].filter(({ stats }) => Object.keys(stats).length > 0))
const pressure = computed(() => Object.entries(object(observations.value.node_pressure_after_initial_put)))
const pressureColumn = computed(() => pressureColumns.find(column => column.key === pressureMetric.value) ?? pressureColumns[0])
const pressureValues = computed(() => pressure.value.map(([, value]) => number(object(value)[pressureMetric.value])))
const hasIdleChart = computed(() => idleRows.value.some(({ stats }) => percentiles.some(key => number(stats[key]) !== null)))
const scalarObservations = computed(() => Object.entries(observations.value)
  .filter(([, value]) => value !== null && typeof value !== 'object'))

function latency(stage: Measurement | undefined, operation: string): Latency {
  return object(object(stage?.latency_us)[operation]) as Latency
}

const axisStyle = { axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: '#736f65', fontSize: 11 }, splitLine: { lineStyle: { color: '#eee9e1', type: 'dashed' } } }
const curveOption = computed(() => latencyCurveOption(rateStages.value, curveOperation.value))
const hasCurve = computed(() => hasLatencyCurve(rateStages.value, curveOperation.value))
function latencyBars(rows: { label: string; stats: Record<string, unknown> }[]) {
  return {
    color: [colors[1], colors[2], colors[0]],
    grid: { top: 42, left: 40, right: 18, bottom: 24, outerBoundsMode: 'same' },
    tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => `${fmt(value, 2)} µs` },
    legend: { top: 0, right: 6, itemWidth: 9, itemHeight: 9, textStyle: { color: '#736f65' } },
    xAxis: { ...axisStyle, type: 'category', data: percentiles, splitLine: { show: false } },
    yAxis: { ...axisStyle, type: 'value', name: 'µs' },
    series: rows.map(({ label, stats }) => ({
      name: label, type: 'bar', barMaxWidth: 18,
      itemStyle: { borderRadius: [3, 3, 0, 0] }, data: percentiles.map(key => number(stats[key])),
    })),
  }
}
const distributionOption = computed(() => latencyBars(distribution.value.map(([name, stats]) => ({ label: name.toUpperCase(), stats: object(stats) }))))
const idleOption = computed(() => latencyBars(idleRows.value))
const pressureOption = computed(() => ({
  grid: { top: 12, left: 6, right: 30, bottom: 26, outerBoundsMode: 'same' },
  tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => `${fmt(value)} ${pressureColumn.value.unit}` },
  xAxis: { ...axisStyle, type: 'value', min: 0, minInterval: 1, name: pressureColumn.value.unit },
  yAxis: { ...axisStyle, type: 'category', inverse: true, data: pressure.value.map(([node]) => node) },
  series: [{
    name: pressureColumn.value.label, type: 'bar', barMaxWidth: 22, itemStyle: { color: colors[0], borderRadius: [0, 4, 4, 0] },
    label: { show: true, position: 'right', formatter: (item: { value: unknown }) => fmt(item.value) },
    data: pressureValues.value,
  }],
}))
const loadOption = computed(() => nodeLoadOption(nodeLoads.value, loadMetric.value))
</script>

<template>
  <div class="overview">
    <template v-if="section !== 'observations' && rateStages.length">
      <div class="section-heading stage-heading"><div><h3>{{ section === 'nodes' ? '阶段节点分布' : '阶段指标' }}</h3><p>正式计时 {{ fmt(current?.seconds, 2) }} s，不含预热。</p></div><label class="inline-label">当前阶段<select :value="currentEntry?.[0]" @change="emit('selectStage', ($event.target as HTMLSelectElement).value)"><option v-for="[name, stage] in rateStages" :key="name" :value="name">{{ name }} · {{ fmt(stage.target_rate) }} ops/s</option></select></label></div>
      <div v-if="current?.sender_saturated === true" class="notice warning" role="status">发送端未跟上计划。本阶段不能按目标负载解释为集群表现。</div>
      <template v-if="section === 'latency'">
        <nav class="view-switch" aria-label="延迟内容"><button :aria-pressed="view === 'curve'" @click="view = 'curve'">负载曲线</button><button :aria-pressed="view === 'distribution'" @click="view = 'distribution'">延迟分布</button><button :aria-pressed="view === 'numbers'" @click="view = 'numbers'">数值明细</button></nav>
        <div class="metric-strip latency-metrics"><div v-for="op in operations" :key="op" class="metric"><span>{{ op.toUpperCase() }} p99</span><strong>{{ fmt(latency(current, op).p99, 2) }}<small>µs</small></strong><span>{{ fmt(latency(current, op).samples) }} 个样本</span></div></div>
        <section v-if="view === 'curve'" class="panel curve-panel">
          <div class="section-heading"><div><h3>延迟随负载的变化</h3><p>横轴为各阶段实际成功速率。</p></div><div class="segmented" aria-label="曲线操作类型"><button v-for="op in operations" :key="op" :aria-pressed="curveOperation === op" @click="curveOperation = op">{{ op.toUpperCase() }}</button></div></div>
          <DataChart v-if="hasCurve" :option="curveOption" :label="curveOperation.toUpperCase() + ' 阶段 p50 和 p99 延迟与实际速率曲线'" :source="run.id" exportable /><p v-else class="empty-inline">所选操作没有可绘制的阶段延迟数据。</p>
          <details class="data-disclosure"><summary>各阶段计数与错误 · 展开核对</summary><div class="table-scroll"><table class="stage-table"><caption class="sr-only">各阶段速率、计数和所选操作延迟</caption><thead><tr><th>阶段</th><th>目标 / 实际 ops/s</th><th>{{ curveOperation.toUpperCase() }} p50 / p99 µs</th><th>issued / late</th><th>重试</th><th>错误</th><th>发送端</th></tr></thead><tbody><tr v-for="[name, stage] in rateStages" :key="name" :class="{ 'selected-row': currentEntry?.[0] === name }"><th scope="row"><button class="table-link" :aria-pressed="currentEntry?.[0] === name" @click="emit('selectStage', name)">{{ name }}</button></th><td>{{ fmt(stage.target_rate) }} / {{ fmt(stage.achieved_rate, 2) }}</td><td>{{ fmt(latency(stage, curveOperation).p50) }} / {{ fmt(latency(stage, curveOperation).p99) }}</td><td>{{ fmt(stage.issued) }} / {{ fmt(stage.late) }}</td><td>{{ fmt(stage.retries) }}</td><td class="wrap-cell">{{ errorsText(stage.errors) }}</td><td>{{ stage.sender_saturated === true ? '饱和' : stage.sender_saturated === false ? '正常' : '未提供' }}</td></tr></tbody></table></div></details>
        </section>
        <section v-else-if="view === 'distribution'" class="panel"><div class="section-heading"><div><h3>延迟分布</h3><p>{{ currentEntry?.[0] }} · 客户端端到端延迟，单位 µs。</p></div></div><DataChart v-if="distribution.length" :option="distributionOption" label="各操作的 p50、p95、p99 和 max 延迟（µs）" :source="run.id + ' / ' + currentEntry?.[0]" exportable /><p v-else class="empty-inline">该阶段未提供延迟分布。</p></section>
        <section v-else class="panel"><div class="table-scroll"><table><caption class="sr-only">当前阶段延迟、成功操作数与样本数</caption><thead><tr><th>操作</th><th>p50</th><th>p95</th><th>p99</th><th>p999</th><th>max</th><th>max_observed</th><th>超量程</th><th>样本数</th><th>成功操作数</th></tr></thead><tbody><tr v-for="[name, stats] in distribution" :key="name"><th scope="row">{{ name.toUpperCase() }}</th><td v-for="key in (['p50', 'p95', 'p99', 'p999', 'max', 'max_observed', 'over_range', 'samples'] as const)" :key="key">{{ fmt(stats[key]) }}</td><td>{{ fmt(current?.ops?.[name]) }}</td></tr><tr v-if="!distribution.length"><td colspan="10">未提供延迟数据</td></tr></tbody></table></div><p class="footnote">单位 µs；max 为直方图桶上界。“—”表示未提供。样本包含返回的失败请求；成功操作数单列。</p></section>
      </template>
      <section v-else class="panel">
        <div class="section-heading"><div><h3>节点负载</h3><p>{{ currentEntry?.[0] }} · {{ loadLabels[loadMetric] }}</p></div><select v-model="loadMetric" aria-label="节点负载指标"><option v-for="(label, metric) in loadLabels" :key="metric" :value="metric">{{ label }}</option></select></div>
        <DataChart v-if="nodeLoads.some(([, node]) => nodeLoadValue(node, loadMetric) !== null)" :option="loadOption" :animate-updates="true" :label="loadLabels[loadMetric] + '节点分布柱状图'" :source="run.id + ' / ' + currentEntry?.[0]" exportable /><p v-else class="empty-inline">该阶段未采集此负载指标。</p>
        <div v-if="nodeLoads.length" class="table-scroll node-table"><table><caption class="sr-only">当前阶段每个节点的负载</caption><thead><tr><th>节点</th><th>Owner 键数</th><th>处理操作数</th><th>CPU（单核 %）</th></tr></thead><tbody><tr v-for="[name, node] in nodeLoads" :key="name"><th scope="row">{{ name }}</th><td>{{ fmt(node?.owned_keys) }}</td><td>{{ fmt(node?.ops_handled) }}</td><td>{{ fmt(node?.cpu_percent, 1) }}</td></tr></tbody></table></div>
        <dl v-if="current?.load_balance" class="balance-stats"><div><dt>键数 max/mean</dt><dd>{{ fmt(current.load_balance.owned_keys_max_over_mean, 4) }}</dd></div><div><dt>键数 CV</dt><dd>{{ fmt(current.load_balance.owned_keys_cv, 4) }}</dd></div><div><dt>操作数 max/mean</dt><dd>{{ fmt(current.load_balance.ops_handled_max_over_mean, 4) }}</dd></div><div><dt>操作数 CV</dt><dd>{{ fmt(current.load_balance.ops_handled_cv, 4) }}</dd></div></dl>
        <p class="footnote">Owner 键数不重复计入 follower 副本；均衡统计量由实验程序提供。</p>
      </section>
    </template>
    <div v-else-if="section !== 'observations'" class="empty-block"><h3>暂无阶段指标</h3><p>此运行未提供固定速率阶段。其他观测与原始文件仍可从主页面打开。</p></div>
    <template v-else>
      <nav class="view-switch" aria-label="观测内容"><button :aria-pressed="observationView === 'throughput'" @click="observationView = 'throughput'">批量吞吐</button><button :aria-pressed="observationView === 'idle'" @click="observationView = 'idle'">空闲基线</button><button :aria-pressed="observationView === 'pressure'" @click="observationView = 'pressure'">节点压力</button><button :aria-pressed="observationView === 'other'" @click="observationView = 'other'">其他记录</button><button :aria-pressed="observationView === 'structured'" @click="observationView = 'structured'">模式记录</button></nav>
      <section v-if="observationView === 'throughput'" class="panel"><div class="section-heading"><div><h3>批量步骤吞吐</h3><p>预加载、校验等步骤，与固定速率负载分别记录。</p></div></div><div v-if="throughput.length" class="table-scroll"><table><caption class="sr-only">批量操作吞吐</caption><thead><tr><th>步骤</th><th>操作数</th><th>耗时 s</th><th>吞吐 ops/s</th></tr></thead><tbody><tr v-for="[name, value] in throughput" :key="name"><th scope="row">{{ fieldNames[name] || name }}</th><td>{{ fmt(value.operations) }}</td><td>{{ fmt(value.seconds, 4) }}</td><td>{{ fmt(value.operations_per_second, 2) }}</td></tr></tbody></table></div><p v-else class="empty-inline">未提供批量吞吐。</p></section>
      <section v-else-if="observationView === 'idle'" class="panel">
        <div class="section-heading"><div><h3>空闲基线</h3><p>负载结束后，在空闲集群测得的 PUT 延迟（µs）。</p></div></div>
        <DataChart v-if="hasIdleChart" :option="idleOption" label="空闲基线 PUT 延迟分位数（µs）" :source="run.id" exportable />
        <div v-if="idleRows.length" class="table-scroll">
          <table>
            <caption class="sr-only">空闲基线延迟，单位 µs</caption>
            <thead><tr><th>路径</th><th v-for="key in percentiles" :key="key">{{ key }}</th><th>样本数</th></tr></thead>
            <tbody>
              <tr v-for="{ label, stats } in idleRows" :key="label">
                <th scope="row">{{ label }}</th>
                <td v-for="key in percentiles" :key="key">{{ fmt(stats[key]) }}</td>
                <td>{{ fmt(stats.samples) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p v-else class="empty-inline">未提供空闲基线。</p>
      </section>
      <section v-else-if="observationView === 'pressure'" class="panel">
        <div class="section-heading">
          <div><h3>初始写入后的节点压力</h3><p>去重回执占用与复制状态。</p></div>
          <select v-if="pressure.length" v-model="pressureMetric" aria-label="节点压力指标">
            <option v-for="column in pressureColumns" :key="column.key" :value="column.key">{{ column.label }}（{{ column.unit }}）</option>
          </select>
        </div>
        <template v-if="pressure.length">
          <DataChart v-if="pressureValues.some(value => value !== null)" :option="pressureOption" :label="pressureColumn.label + '（' + pressureColumn.unit + '）'" :source="run.id" :animate-updates="true" exportable />
          <p v-else class="empty-inline">未采集所选压力指标。</p>
          <div class="table-scroll">
            <table>
              <caption class="sr-only">初始写入后的节点压力</caption>
              <thead><tr><th>节点</th><th v-for="column in pressureColumns" :key="column.key">{{ column.label }}（{{ column.unit }}）</th></tr></thead>
              <tbody>
                <tr v-for="[name, value] in pressure" :key="name">
                  <th scope="row">{{ name }}</th>
                  <td v-for="column in pressureColumns" :key="column.key">{{ fmt(object(value)[column.key]) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </template>
        <p v-else class="empty-inline">未提供初始节点压力。</p>
      </section>
      <section v-else-if="observationView === 'other'" class="panel"><div class="section-heading"><div><h3>其他观测</h3><p>实验程序记录的结果。</p></div></div><dl v-if="scalarObservations.length" class="config-grid"><div v-for="[key, value] in scalarObservations" :key="key"><dt>{{ fieldNames[key] || key }}</dt><dd>{{ typeof value === 'number' ? fmt(value, 4) : text(value) }}</dd></div></dl><p v-else class="empty-inline">未提供其他标量观测。</p></section>
      <ModeResults v-else :run="run" />
    </template>
  </div>
</template>
