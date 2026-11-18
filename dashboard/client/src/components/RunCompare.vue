<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import DataChart from './DataChart.vue'
import type { Measurement, RunDetail, RunListItem } from '../types'
import { colors, errorsText, fieldNames, formatNumber as fmt, latencyStatistic, number, object, operations, request, shortName, stages, statusLabels, text } from '../data'
import { canonical, comparisonRates, configRows, numericFields, recordedMetrics, repeatStats, sameExperimentConfig, stagesAtRate } from '../compare.ts'

const props = defineProps<{ runs: RunListItem[]; initialId: string; initialSelection?: string[] }>()
const selected = ref<string[]>(props.initialSelection ?? [props.initialId, ...props.runs.map(run => run.id).filter(id => id !== props.initialId)].filter(Boolean).slice(0, 2))
const applied = ref<string[]>([])
const loaded = ref<RunDetail[]>([])
const failures = ref<{ id: string; message: string }[]>([])
const loading = ref(false)
const view = defineModel<string>('view', { default: 'latency' })
const onlyDifferent = ref(true)
const operation = ref('get')
const percentile = ref<'p50' | 'p95' | 'p99' | 'max'>('p99')
const plot = defineModel<string>('plot', { default: 'bar' })
const groupRepeats = defineModel<boolean>('groupRepeats', { default: false })
const rate = ref<number | null>(null)
const stageOverrides = ref<Record<string, string>>({})
const metric = ref('')
let controller: AbortController | undefined
const rates = computed(() => comparisonRates(loaded.value))
const currentRate = computed(() => rate.value !== null && rates.value.includes(rate.value) ? rate.value : rates.value[0] ?? null)
const current = computed(() => loaded.value.map(run => { const candidates = stagesAtRate(run, currentRate.value); return candidates.find(([key]) => key === stageOverrides.value[run.id]) ?? candidates[0] }))
const configs = computed(() => configRows(loaded.value))
const visibleConfigs = computed(() => configs.value.filter(row => !onlyDifferent.value || row.different))
const fields = computed(() => loaded.value.map(run => numericFields(recordedMetrics(run))))
const metricPaths = computed(() => [...new Set(fields.value.flatMap(fields => [...fields.keys()]))].sort((a, b) => a.localeCompare(b, 'en', { numeric: true })))
const currentMetric = computed(() => metricPaths.value.includes(metric.value) ? metric.value : metricPaths.value[0] ?? '')
function label(run: RunDetail, index: number) {
  const name = shortName(run.name)
  return `${index + 1}. ${name.length > 26 ? name.slice(0, 23) + '…' : name}`
}
const latency = (stage: Measurement | undefined, key: string) => latencyStatistic(stage?.latency_us?.[operation.value], key)
const pending = computed(() => canonical(selected.value) !== canonical(applied.value))
const percentiles = ['p50', 'p95', 'p99', 'max'] as const
const stageValues = computed(() => current.value.map(entry => latency(entry?.[1], percentile.value)))
const baseline = computed(() => stageValues.value[0] ?? null)
const repeatConfig = computed(() => !failures.value.length && sameExperimentConfig(loaded.value))
const repeatStage = computed(() => repeatConfig.value && current.value.every(entry => entry && entry[0] === current.value[0]?.[0] && entry[1].sender_saturated !== true))
const repeatRows = computed(() => {
  if (view.value === 'other' && repeatConfig.value) return [{ name: currentMetric.value, stats: repeatStats(fields.value.map(row => row.get(currentMetric.value))) }]
  if (view.value !== 'latency' || !repeatStage.value) return []
  return [
    { name: '实际成功速率（ops/s）', stats: repeatStats(current.value.map(entry => entry?.[1].achieved_rate)) },
    ...percentiles.map(key => ({ name: `${operation.value.toUpperCase()} ${key}（µs）`, stats: repeatStats(current.value.map(entry => latency(entry?.[1], key))) })),
  ]
})
const chartSource = computed(() => loaded.value.map(run => run.id).join(' / '))
watch(rate, () => { stageOverrides.value = {} })

function measuredRow(name: string, values: (number | null)[]) {
  const reference = values[0] ?? null
  return {
    name,
    values: values.map(value => fmt(value, 2)),
    deltas: values.map((value, index) => index === 0 || value === null || reference === null ? null : value - reference),
  }
}

const metricRows = computed<{ name: string; values: string[]; deltas?: (number | null)[] }[]>(() => [
  measuredRow('实际成功速率 · ops/s', current.value.map(entry => number(entry?.[1].achieved_rate))),
  ...percentiles.map(key => measuredRow(`${operation.value.toUpperCase()} ${key} · µs`, current.value.map(entry => latency(entry?.[1], key)))),
  { name: `${operation.value.toUpperCase()} 样本数`, values: current.value.map(entry => fmt(latency(entry?.[1], 'samples'))) },
  ...(['issued', 'late', 'retries'] as const).map(key => ({ name: key, values: current.value.map(entry => fmt(entry?.[1][key])) })),
  { name: '错误类别', values: current.value.map(entry => entry ? errorsText(entry[1].errors) : '未提供') },
  { name: '发送端', values: current.value.map(entry => entry?.[1].sender_saturated === true ? '饱和' : entry?.[1].sender_saturated === false ? '正常' : '未提供') },
])
const curveOption = computed(() => ({
  color: colors, grid: { top: 20, left: 46, right: 20, bottom: 48, outerBoundsMode: 'same' },
  tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => `${fmt(value, 2)} µs` },
  xAxis: { type: 'value', name: '实际成功速率（ops/s）', nameLocation: 'middle', nameGap: 30 },
  yAxis: { type: 'value', name: 'µs', splitLine: { lineStyle: { color: '#e5e0d7', type: 'dashed' } } },
  series: loaded.value.map((run, index) => ({ name: label(run, index), type: 'line', symbol: 'circle', symbolSize: 7, smooth: false, connectNulls: false,
    lineStyle: { width: 2, type: index >= colors.length ? 'dashed' : 'solid' },
    data: stages(run.summary).filter(([, stage]) => number(stage.achieved_rate) !== null).map(([, stage]) => [stage.achieved_rate, latency(stage, percentile.value)]).sort((a, b) => (a[0] ?? 0) - (b[0] ?? 0)),
  })),
}))
function bars(values: (number | null)[], unit: string) {
  return { grid: { top: 12, left: 3, right: 65, bottom: 30, outerBoundsMode: 'same' }, tooltip: { trigger: 'axis' },
    xAxis: { type: 'value', name: unit, min: 0 }, yAxis: { type: 'category', inverse: true, data: loaded.value.map(label), axisTick: { show: false }, axisLine: { show: false } },
    series: [{ id: 'run-bars', type: 'bar', barMaxWidth: 28, showBackground: true, backgroundStyle: { color: '#f3f1ec' }, label: { show: true, position: 'right', formatter: (item: { value: unknown }) => fmt(item.value, 2) },
      data: values.map((value, index) => ({ name: loaded.value[index]?.id, value, itemStyle: { color: colors[index % colors.length], borderRadius: [0, 4, 4, 0] } })),
    }],
  }
}
const barOption = computed(() => bars(stageValues.value, 'µs'))
const groupedOption = computed(() => ({
  color: colors, grid: { top: 14, left: 8, right: 42, bottom: 35, outerBoundsMode: 'same' },
  tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => `${fmt(value, 2)} µs` },
  xAxis: { type: 'value', name: 'µs', min: 0 },
  yAxis: { type: 'category', inverse: true, data: percentiles, axisTick: { show: false }, axisLine: { show: false } },
  series: loaded.value.map((run, index) => ({
    id: run.id, name: label(run, index), type: 'bar', barMaxWidth: 14,
    data: percentiles.map(key => latency(current.value[index]?.[1], key)),
  })),
}))
const otherOption = computed(() => bars(fields.value.map(fields => fields.get(currentMetric.value) ?? null), '原字段单位'))

async function load() {
  if (selected.value.length < 2) return
  controller?.abort()
  controller = new AbortController()
  const signal = controller.signal
  const ids = [...selected.value]
  loading.value = true
  failures.value = []
  const results = await Promise.allSettled(ids.map(id => request<RunDetail>(`/api/runs/${encodeURIComponent(id)}`, signal)))
  if (signal.aborted) return
  loaded.value = results.flatMap(result => result.status === 'fulfilled' ? [result.value] : [])
  failures.value = results.flatMap((result, index) => result.status === 'rejected' ? [{ id: ids[index]!, message: result.reason instanceof Error ? result.reason.message : String(result.reason) }] : [])
  applied.value = ids
  stageOverrides.value = {}
  loading.value = false
}
onMounted(load)
onBeforeUnmount(() => controller?.abort())
</script>

<template>
  <section class="compare-view">
    <details class="data-disclosure compare-picker" :open="!applied.length && selected.length < 2"><summary>选择运行 · 已选 {{ selected.length }} 次</summary><div class="comparison-choices"><label v-for="run in runs" :key="run.id" class="comparison-choice"><input v-model="selected" type="checkbox" :value="run.id" /><span><strong>{{ shortName(run.name) }}</strong><code>{{ run.id }}</code><small>{{ text(run.config.mode) }} · {{ statusLabels[run.status] }}</small></span></label></div><button class="button primary" :disabled="selected.length < 2 || loading" @click="load">{{ loading ? '正在读取' : '应用选择' }}</button><p v-if="selected.length < 2" class="footnote">至少选择两次运行。</p></details>
    <div v-if="pending && !loading" class="notice neutral"><p>选择已改变，点击“应用选择”更新对比。</p><button class="table-link" :disabled="selected.length < 2" @click="load">应用选择</button></div>
    <p v-if="loading" class="empty-inline" role="status">正在读取所选运行……</p>
    <div v-for="failure in failures" :key="failure.id" class="notice error" role="alert"><p><strong>{{ failure.id }}</strong><span>{{ failure.message }}；其他可读运行已保留。</span></p><button class="table-link" @click="load">重试</button></div>
    <template v-if="loaded.length && !loading">
      <div class="comparison-legend"><div v-for="(run, index) in loaded" :key="run.id" :title="run.id"><i :style="{ background: colors[index % colors.length] }"></i><span>{{ label(run, index) }}</span><span class="status-badge" :class="run.status">{{ statusLabels[run.status] }}</span><small v-if="run.manifest?.source_reproducible === false" class="text-warning">源码待核对</small><small v-if="run.warnings.length" class="text-warning">{{ run.warnings.length }} 项文件提示</small></div></div>
      <p v-if="loaded.length < 2" class="notice warning">只有一次运行可读取，请调整选择或重试后进行对比。</p>
      <nav class="view-switch" aria-label="对比内容"><button :aria-pressed="view === 'latency'" @click="view = 'latency'">阶段指标</button><button :aria-pressed="view === 'other'" @click="view = 'other'">其他指标</button><button :aria-pressed="view === 'config'" @click="view = 'config'">配置差异</button></nav>
      <template v-if="view === 'config'"><label class="checkbox-label compare-diff"><input v-model="onlyDifferent" type="checkbox" />只看不同参数</label><div class="table-scroll"><table><caption class="sr-only">所选运行配置差异</caption><thead><tr><th>参数</th><th v-for="(run, index) in loaded" :key="run.id" :title="run.id">{{ label(run, index) }}</th></tr></thead><tbody><tr v-for="row in visibleConfigs" :key="row.key" :class="{ 'different-row': row.different }"><th scope="row">{{ fieldNames[row.key] || row.key }}<code class="field-code">{{ row.key }}</code></th><td v-for="(value, index) in row.values" :key="index" class="wrap-cell">{{ text(value) }}</td></tr><tr v-if="!visibleConfigs.length"><td :colspan="loaded.length + 1">{{ onlyDifferent ? '没有配置差异，可取消勾选查看全部参数。' : '没有记录配置。' }}</td></tr></tbody></table></div></template>
      <template v-else-if="view === 'latency'">
        <template v-if="rates.length"><div class="comparison-controls"><label>目标速率<select :value="currentRate" aria-label="对比目标速率" @change="rate = Number(($event.target as HTMLSelectElement).value)"><option v-for="value in rates" :key="value" :value="value" :selected="currentRate === value">{{ fmt(value) }} ops/s</option></select></label><div class="segmented" aria-label="对比操作"><button v-for="op in operations" :key="op" :aria-pressed="operation === op" @click="operation = op">{{ op.toUpperCase() }}</button></div><label>分位值<select v-model="percentile" aria-label="对比分位值"><option v-for="value in percentiles" :key="value">{{ value }}</option></select></label><select v-model="plot" aria-label="对比图类型"><option value="bar">所选速率条形图</option><option value="group">分组分位数</option><option value="curve">全部阶段曲线</option></select></div>
          <div v-if="loaded.length > 1 && plot !== 'curve'" :key="[currentRate, operation, percentile].join('-')" class="comparison-deltas">
            <div v-for="(value, index) in stageValues" :key="loaded[index]?.id"><span>{{ label(loaded[index]!, index) }}</span><strong>{{ fmt(value, 2) }} <small>µs</small></strong><small>{{ index === 0 ? '对照运行' : baseline === null || value === null ? '无可比读数' : (value > baseline ? '+' : '') + fmt(value - baseline, 2) + ' µs 相对对照' }}</small></div>
          </div>
          <DataChart :key="plot" :option="plot === 'curve' ? curveOption : plot === 'group' ? groupedOption : barOption" :animate-updates="plot !== 'curve'" :label="operation.toUpperCase() + ' ' + (plot === 'group' ? '各分位数' : percentile) + ' 延迟对比' + (plot === 'curve' ? '（µs）' : ' / 目标 ' + fmt(currentRate) + ' ops/s（µs）')" :source="chartSource" exportable />
          <p class="footnote">{{ plot === 'curve' ? '横轴为实际成功速率。下表按所选目标速率匹配。' : '图表和表格按所选目标速率匹配。差值以第一条运行为对照。' }}</p>
          <div class="table-scroll"><table><caption class="sr-only">所选速率各运行指标</caption><thead><tr><th>指标</th><th v-for="(run, index) in loaded" :key="run.id" :title="run.id">{{ label(run, index) }}<span v-if="index === 0" class="table-delta">对照</span><span class="field-code">{{ current[index]?.[0] || '没有匹配阶段' }}</span><select v-if="stagesAtRate(run, currentRate).length > 1" :value="current[index]?.[0]" aria-label="选择重复目标阶段" @change="stageOverrides[run.id] = ($event.target as HTMLSelectElement).value"><option v-for="[name] in stagesAtRate(run, currentRate)" :key="name" :value="name">{{ name }}</option></select></th></tr></thead><tbody><tr v-for="row in metricRows" :key="row.name"><th scope="row">{{ row.name }}</th><td v-for="(value, index) in row.values" :key="index" class="wrap-cell">{{ value }}<small v-if="row.deltas?.[index] != null" class="table-delta" :title="'相对对照运行'">{{ row.deltas[index]! > 0 ? '+' : '' }}{{ fmt(row.deltas[index], 2) }}</small></td></tr></tbody></table></div>
        </template><p v-else class="empty-inline">所选运行未提供固定速率阶段，可切换“其他指标”或“配置差异”。</p>
      </template>
      <template v-else><template v-if="currentMetric"><label class="record-selector">原始字段<select :value="currentMetric" aria-label="选择对比指标" @change="metric = ($event.target as HTMLSelectElement).value"><option v-for="path in metricPaths" :key="path" :value="path" :selected="path === currentMetric">{{ path }}</option></select></label><DataChart :option="otherOption" :animate-updates="true" :label="currentMetric + ' 多运行原始指标对比'" :source="chartSource" exportable /><div class="table-scroll"><table><caption class="sr-only">原始指标值</caption><thead><tr><th>运行</th><th>{{ currentMetric }}</th></tr></thead><tbody><tr v-for="(run, index) in loaded" :key="run.id"><th scope="row">{{ label(run, index) }}</th><td>{{ fmt(fields[index]?.get(currentMetric), 6) }}</td></tr></tbody></table></div><p class="footnote">按完整字段路径匹配。_ms 为毫秒、_seconds 为秒、_us 为微秒。比较故障轮次前需核对场景一致。</p></template><p v-else class="empty-inline">没有可比较的数值记录。</p></template>
      <section v-if="view !== 'config' && loaded.length > 1" id="compare-repeats" tabindex="-1" class="repeat-summary">
        <label class="checkbox-label"><input v-model="groupRepeats" type="checkbox" />将所选运行作为重复组</label>
        <template v-if="groupRepeats">
          <div v-if="repeatRows.length" class="table-scroll"><table><caption class="sr-only">重复运行描述统计</caption><thead><tr><th>指标</th><th>记录数</th><th>均值</th><th>最小值</th><th>最大值</th></tr></thead><tbody><tr v-for="row in repeatRows" :key="row.name"><th scope="row">{{ row.name }}</th><td>{{ row.stats?.count ?? 0 }} / {{ loaded.length }}</td><td>{{ fmt(row.stats?.mean, 2) }}</td><td>{{ fmt(row.stats?.min, 2) }}</td><td>{{ fmt(row.stats?.max, 2) }}</td></tr></tbody></table></div>
          <p v-else class="notice neutral">重复组要求所有运行成功、配置一致，并有同名阶段且未记录发送端饱和。</p>
          <p class="footnote">只统计已有读数。分位数均值描述各次成绩，原始记录仍逐次展示。</p>
        </template>
      </section>
      <details class="data-disclosure"><summary>运行身份与结果提示</summary><div v-for="run in loaded" :key="run.id" class="comparison-source"><code>{{ run.id }}</code><p>模式 {{ text(run.manifest?.config?.mode) }} · {{ statusLabels[run.status] }} · 总耗时 {{ fmt(run.summary?.elapsed_seconds, 3) }} s</p><p v-if="run.summary?.error" class="text-warning">{{ run.summary.error }}</p><p v-for="warning in run.warnings" :key="warning" class="text-warning">{{ warning }}</p></div></details>
      <p class="footnote">读数来自各自的 summary。比较前可在“配置差异”核对实验条件。</p>
    </template>
  </section>
</template>
