<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue'
import DataChart from './DataChart.vue'
import LatencyMetricSelector from './LatencyMetricSelector.vue'
import ReplayEventList from './ReplayEventList.vue'
import StageComparison from './StageComparison.vue'
import TopologyView from './TopologyView.vue'
import type { EventLog, ExperimentFeed, ReplayStart, RunDetail, RunEvent } from '../types'
import { colors, errorsText, formatNumber as fmt, number, object, operations, request, stages, text } from '../data'
import { absoluteEventTime, eventCategory, eventName, eventSummary, isKeyEvent, relativeEventTime } from '../events'
import { nodeOperationSample, replayBounds, sampledSeries, visibleCount, warmupIntervals, windowLatency, windowSuccessRate } from '../history.ts'
import { defaultLatencyCurveMetrics, latencyCurveMetricKeys, latencyCurveMetrics, visibleLatencyCurveMetrics, type LatencyCurveMetric } from '../loadCharts'
import { useReplayClock } from '../useReplayClock'

const props = defineProps<{ run?: RunDetail; live?: ExperimentFeed; caseStudy?: boolean; initial?: ReplayStart }>()
const history = shallowRef<EventLog>(props.initial?.log ?? { events: [], issues: [], missing: false })
const log = computed(() => props.live?.log ?? history.value)
const followLive = computed(() => !!props.live && props.live.session?.endedAt == null)
const loading = ref(!props.live && !props.initial)
const error = ref('')
const hideWarmup = ref(false)
const markers = ref(true)
const plot = ref('latency')
const stageRows = computed(() => stages(props.run?.summary ?? null))
const stageByName = computed(() => new Map(stageRows.value))
const view = defineModel<string>('view', { default: 'metrics' })
const showStages = computed(() => !props.live && stageRows.value.length > 0 && view.value === 'stages')
const operation = ref('get')
const selectedMetrics = ref<LatencyCurveMetric[]>([...defaultLatencyCurveMetrics])
const inspectLine = ref<number | null>(null)
const chart = ref<InstanceType<typeof DataChart> | null>(null)
const layoutVersion = ref(0)
let controller: AbortController | undefined
const bounds = computed(() => replayBounds(log.value.events, props.run?.manifest?.started_unix_ms ?? props.live?.session?.startedAt))
const { playing, speed, cursor, boundaryLine, pause, seek, toggle } = useReplayClock(() => bounds.value)
if (props.initial) seek(props.initial.time, props.initial.line)
const time = computed(() => followLive.value ? bounds.value.end : cursor.value)
const count = computed(() => visibleCount(log.value.events, time.value, boundaryLine.value ?? Infinity))
const seen = computed(() => log.value.events.slice(0, count.value))
const currentEvent = computed(() => seen.value.at(-1))
const inspectedEvent = computed(() => log.value.events.find(event => event.line === inspectLine.value) ?? currentEvent.value)
const allSamples = computed(() => log.value.events.filter(event => event.event === 'metrics_sampled'))
const availableMetrics = computed(() => latencyCurveMetricKeys.filter(metric =>
  allSamples.value.some(event => windowLatency(event, operation.value, metric) !== null)))
const visibleMetrics = computed(() => visibleLatencyCurveMetrics(availableMetrics.value, selectedMetrics.value))
const showWarmupLegend = computed(() => !hideWarmup.value && allSamples.value.some(event => event.fields.warmup === true))
const sampleCount = computed(() => visibleCount(allSamples.value, time.value, boundaryLine.value ?? Infinity))
const samples = computed(() => allSamples.value.slice(0, sampleCount.value).filter(event => !hideWarmup.value || event.fields.warmup !== true))
const latestSample = computed(() => samples.value.at(-1))
const topologyRecords = computed(() => log.value.events.filter(event => event.event === 'topology_published'))
const topologyCount = computed(() => visibleCount(topologyRecords.value, time.value, boundaryLine.value ?? Infinity))
const seenTopology = computed(() => topologyRecords.value.slice(0, topologyCount.value))
const latestTopology = computed(() => seenTopology.value.at(-1))
function sampleNodeOperations(event: RunEvent) {
  return nodeOperationSample(event, stageByName.value.get(text(event.fields.stage)))
}
const nodeSamples = computed(() => log.value.events.flatMap(event => {
  if (event.event !== 'metrics_sampled' && event.event !== 'node_load_sampled') return []
  const load = sampleNodeOperations(event)
  return load ? [{ event, load }] : []
}))
const visibleNodeSamples = computed(() => hideWarmup.value
  ? nodeSamples.value.filter(({ event }) => event.fields.warmup !== true)
  : nodeSamples.value)
const nodeSampleEvents = computed(() => visibleNodeSamples.value.map(({ event }) => event))
const nodeSampleCount = computed(() => visibleCount(nodeSampleEvents.value, time.value, boundaryLine.value ?? Infinity))
function matchesEpoch(event: RunEvent) {
  const epoch = latestTopology.value?.fields.epoch
  return epoch != null && String(event.fields.epoch) === String(epoch)
}
const nodeLoadScale = computed(() => {
  let peak = 1
  for (const { load } of nodeSamples.value) {
    for (const value of load.values.values()) peak = Math.max(peak, value)
  }
  const step = 10 ** Math.max(0, Math.floor(Math.log10(peak)) - 1)
  return Math.ceil(peak / step) * step
})
const topologySample = computed(() => {
  const sample = latestSample.value
  return sample && matchesEpoch(sample) ? sample : undefined
})
const nodeLoad = computed(() => {
  const reading = visibleNodeSamples.value.slice(0, nodeSampleCount.value).reverse().find(({ event }) => matchesEpoch(event))
  if (!reading) return null
  const { event: sample, load } = reading
  const caption = load.average
    ? `阶段平均 · 目标 ${fmt(sample.fields.target_rate)} ops/s`
    : `采样窗口 · ${relativeEventTime(sample.unix_ms, bounds.value.origin)}`
  return { values: load.values, scale: nodeLoadScale.value, caption }
})
const nextNodeSample = computed(() => followLive.value ? undefined
  : visibleNodeSamples.value.slice(nodeSampleCount.value).find(({ event }) => matchesEpoch(event))?.event.line)
const nodeOwnedKeys = computed(() => {
  const perNode = stageByName.value.get(text(topologySample.value?.fields.stage))?.load_balance?.per_node
  const values = new Map<string, number>()
  for (const [id, node] of Object.entries(perNode ?? {})) {
    const count = number(node?.owned_keys)
    if (count !== null && count >= 0) values.set(id, count)
  }
  return values
})
const recordedEpochs = computed(() => [...new Set(topologyRecords.value.flatMap(event => event.fields.epoch == null ? [] : [text(event.fields.epoch)]))])
const keyEvents = computed(() => log.value.events.filter(isKeyEvent))
const scenario = computed(() => keyEvents.value.some(event => eventCategory(event.event) === 'fault') ? 'fault'
  : keyEvents.value.some(event => eventCategory(event.event) === 'migration') ? 'migration' : 'topology')
const scenarioFacts = computed(() => {
  const source = object(props.run?.summary?.observations)
  return [
    { label: '扩容至完整 RF', value: number(source.scale_out_transition_to_full_rf_seconds) },
    { label: '缩容至完整 RF', value: number(source.scale_in_transition_to_full_rf_seconds) },
    { label: '故障转移', value: number(source.failover_seconds) },
    { label: '读取恢复', value: number(source.read_recovery_seconds) },
    { label: '故障至完整 RF', value: number(source.failure_to_full_rf_seconds) },
  ].filter(item => item.value !== null)
})
const sourceIssues = computed(() => log.value.issues.length)
const duration = computed(() => bounds.value.end - bounds.value.start)
const unit = computed(() => plot.value === 'latency' ? 'µs' : plot.value === 'rate' ? 'ops/s' : '次／窗口')
const seconds = computed(() => (time.value - bounds.value.start) / 1000)
const progress = computed(() => duration.value ? (time.value - bounds.value.start) / duration.value : 0)
const playhead = computed(() => { void layoutVersion.value; return chart.value?.pixelAt(seconds.value) ?? null })
const plottedOperations = computed(() => [...new Set<string>([
  ...operations,
  ...allSamples.value.flatMap(event => Object.keys(object(event.fields.metrics))),
])])
const demonstration = computed(() => {
  const list: { event: RunEvent; label: string }[] = []
  const ring = topologyRecords.value[0]
  if (ring) list.push({ event: ring, label: '初始哈希环' })
  const firstOfStage = new Map<unknown, RunEvent>()
  for (const event of allSamples.value) {
    if (event.fields.target_rate !== undefined && event.fields.warmup !== true && !firstOfStage.has(event.fields.stage)) firstOfStage.set(event.fields.stage, event)
  }
  for (const event of firstOfStage.values()) list.push({ event, label: `${fmt(event.fields.target_rate)} ops/s` })
  for (const event of keyEvents.value) {
    if (['migration_started', 'migration_complete', 'scaling_transition_started', 'scaling_transition_complete', 'owner_killed', 'failover_published', 'full_rf_restored', 'fault_injected', 'fault_published', 'fault_full_rf_restored'].includes(event.event)) {
      list.push({ event, label: eventName(event.event) })
    }
  }
  return list.sort((a, b) => a.event.unix_ms - b.event.unix_ms || a.event.line - b.event.line)
})
const seriesDefs = computed<{ name: string; value: (event: RunEvent) => number | null; metric?: LatencyCurveMetric }[]>(() => {
  if (plot.value === 'latency') return visibleMetrics.value.map(metric => ({
    name: `${operation.value.toUpperCase()} ${latencyCurveMetrics[metric].label}`,
    value: (event: RunEvent) => windowLatency(event, operation.value, metric),
    metric,
  }))
  if (plot.value === 'rate') return [
    { name: '目标速率', value: (event: RunEvent) => number(event.fields.target_rate) },
    { name: '窗口成功速率', value: windowSuccessRate },
    { name: '窗口发出速率（扩缩容）', value: (event: RunEvent) => event.fields.mode === 'scaling' ? number(event.fields.achieved_rate) : null },
  ].filter(series => allSamples.value.some(event => series.value(event) !== null))
  if (plot.value === 'counts') return Object.entries({ issued: '发出', late: '未按时发出', retries: '重试' }).map(([key, name]) => ({ name, value: event => number(event.fields[key]) }))
  const names = [...new Set(allSamples.value.flatMap(event => Object.keys(object(event.fields.errors))))]
  return names.map(key => ({ name: key, value: event => number(object(event.fields.errors)[key]) }))
})
const chartOption = computed(() => ({
  color: plot.value === 'latency' ? visibleMetrics.value.map(metric => latencyCurveMetrics[metric].color) : colors,
  grid: { top: 20, left: 64, right: 22, bottom: 42, outerBoundsMode: 'none' },
  tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => `${fmt(Array.isArray(value) ? value.at(-1) : value, 2)} ${unit.value}` },
  xAxis: { type: 'value', min: 0, max: followLive.value ? Math.max(10, Math.ceil(duration.value / 10000) * 10) : Math.max(1, duration.value / 1000), name: '记录时间（s）', nameLocation: 'middle', nameGap: 22, axisLabel: { hideOverlap: true } },
  yAxis: { type: 'value', min: 0, name: unit.value, nameGap: 6, splitNumber: 4, axisLabel: { fontSize: 11, hideOverlap: true }, splitLine: { lineStyle: { color: '#e5e0d7', type: 'dashed' } } },
  series: seriesDefs.value.map((series, index) => {
    const style = series.metric ? latencyCurveMetrics[series.metric] : undefined
    return {
      name: series.name, type: 'line', symbol: style?.symbol ?? 'circle', symbolSize: 5, smooth: false, connectNulls: false,
      lineStyle: { width: 2, type: style?.lineType ?? (series.name === '目标速率' ? 'dashed' : 'solid') },
      data: sampledSeries(samples.value, bounds.value.start, series.value),
      ...(index === 0 ? {
        markArea: { silent: true, itemStyle: { color: '#ccc6bb40' }, data: warmupIntervals(seen.value, bounds.value.start).map(([from, to]) => [{ xAxis: from }, { xAxis: to }]) },
        markLine: { silent: true, symbol: ['none', 'none'], label: { show: false }, lineStyle: { color: '#736f65', opacity: .35, type: 'dashed' }, data: markers.value ? seen.value.filter(event => ['topology', 'migration', 'fault'].includes(eventCategory(event.event))).map(event => ({ name: eventName(event.event), xAxis: (event.unix_ms - bounds.value.start) / 1000 })) : [] },
      } : {}),
    }
  }),
}))
const hasPlottedValue = computed(() => seriesDefs.value.some(series => samples.value.some(event => series.value(event) !== null)))

function jump(line: number) {
  if (followLive.value) { inspectLine.value = line; return }
  const event = log.value.events.find(event => event.line === line)
  if (event) seek(event.unix_ms, line)
}
function step(direction: number) {
  if (direction < 0) { const event = log.value.events[Math.max(0, count.value - 2)]; if (event) jump(event.line) }
  else { const event = log.value.events[count.value]; if (event) jump(event.line) }
}
async function load() {
  if (!props.run) return
  pause()
  controller?.abort()
  controller = new AbortController()
  const signal = controller.signal
  loading.value = true
  error.value = ''
  try {
    history.value = await request<EventLog>(`/api/runs/${encodeURIComponent(props.run.id)}/events`, signal)
    if (!signal.aborted) seek(props.caseStudy ? bounds.value.end : bounds.value.start)
  }
  catch (failure) { if (!signal.aborted) error.value = failure instanceof Error ? failure.message : String(failure) }
  finally { if (!signal.aborted) loading.value = false }
}
watch(() => props.live?.session?.endedAt, endedAt => {
  if (endedAt == null) return
  inspectLine.value = null
  seek(bounds.value.end)
}, { immediate: true })
onMounted(() => { if (!props.live && !props.initial) void load() })
onBeforeUnmount(() => controller?.abort())
</script>

<template>
  <section class="replay-view">
    <p v-if="loading" class="empty-inline" role="status">正在读取回放记录……</p>
    <div v-else-if="error" class="notice error" role="alert"><p>{{ error }}</p><button class="table-link" @click="load">重试</button></div>
    <template v-else>
      <div v-if="sourceIssues" class="notice warning">{{ sourceIssues }} 行无法解析，视图仅使用有效记录。</div>
      <div class="playback-layout">
        <div class="playback-stage">
          <nav v-if="!live && stageRows.length" class="view-switch" aria-label="回放与阶段对比">
            <button :aria-pressed="showStages" @click="pause(); view = 'stages'">阶段对比</button>
            <button :aria-pressed="!showStages" @click="view = 'metrics'">时间回放</button>
          </nav>

          <StageComparison v-if="showStages && run" class="playback-panel" :run="run" />
          <template v-else>
            <div v-if="!followLive && log.events.length" class="playback-transport" aria-label="回放控制" role="group">
              <div class="replay-controls">
                <button class="button primary" :disabled="!duration" @click="toggle">{{ playing ? '暂停' : cursor >= bounds.end ? '从头播放' : '播放' }}</button>
                <button class="button secondary" aria-label="上一个事件" :disabled="count <= 1" @click="step(-1)">←</button>
                <button class="button secondary" aria-label="下一个事件" :disabled="count >= log.events.length" @click="step(1)">→</button>
              </div>
              <div class="replay-track">
                <div class="replay-position"><span>{{ fmt(seconds, 2) }} / {{ fmt(duration / 1000, 2) }} s</span><span>{{ count }} / {{ log.events.length }} 条</span></div>
                <input class="replay-seek" type="range" aria-label="回放进度" :aria-valuetext="relativeEventTime(cursor, bounds.start)" :min="bounds.start" :max="bounds.end" :value="cursor" :disabled="!duration" :style="{ '--playback-progress': (progress * 100) + '%' }" step="1" @input="seek(Number(($event.target as HTMLInputElement).value))" />
              </div>
              <div class="replay-controls replay-jumps">
                <label><span>速度</span><select v-model="speed" aria-label="回放速度"><option v-for="value in [1, 2, 5, 10]" :key="value" :value="value">{{ value }}×</option></select></label>
                <button class="table-link" @click="seek(bounds.start)">起点</button>
                <button class="table-link" @click="seek(bounds.end)">结束</button>
              </div>
            </div>

            <section id="playback-metrics" tabindex="-1" class="playback-panel playback-plot">
              <div class="playback-panel-heading">
                <h3>指标曲线</h3>
                <div v-if="allSamples.length" class="comparison-controls">
                  <select v-model="plot" aria-label="窗口指标"><option value="latency">窗口延迟</option><option value="rate">目标与实际速率</option><option value="counts">发送与重试</option><option value="errors">错误类别</option></select>
                  <template v-if="plot === 'latency'">
                    <select v-model="operation" aria-label="回放操作"><option v-for="op in plottedOperations" :key="op" :value="op">{{ op.toUpperCase() }}</option></select>
                  </template>
                  <label class="checkbox-label"><input v-model="hideWarmup" type="checkbox" />隐藏预热</label>
                  <label class="checkbox-label"><input v-model="markers" type="checkbox" />事件标记</label>
                </div>
              </div>
              <template v-if="allSamples.length">
                <div class="replay-plot-key">
                  <div v-if="plot !== 'latency' || showWarmupLegend" class="replay-legend">
                    <template v-if="plot !== 'latency'">
                      <span v-for="(series, index) in seriesDefs" :key="series.name"><i :style="{ background: colors[index % colors.length] }"></i>{{ series.name }}</span>
                    </template>
                    <span v-if="showWarmupLegend"><i class="replay-warmup"></i>预热</span>
                  </div>
                  <LatencyMetricSelector v-if="plot === 'latency'" v-model="selectedMetrics" :available="availableMetrics" class="playback-percentiles" />
                </div>
                <div class="replay-chart">
                  <DataChart ref="chart" :option="chartOption" :motion="false" :label="(followLive ? '实时' : '回放') + '窗口记录曲线'" @layout="layoutVersion++" />
                  <div v-if="playhead !== null" class="chart-playhead" :style="{ transform: 'translateX(' + playhead + 'px)' }" aria-hidden="true"></div>
                  <p v-if="!hasPlottedValue" class="chart-empty-label">{{ samples.length ? '所选指标没有记录' : '等待首个采样窗口' }}</p>
                </div>
              </template>
              <p v-else class="empty-inline playback-wait">{{ followLive ? '等待指标采样。' : '本次运行没有指标采样。' }}</p>
            </section>

            <section id="playback-topology" tabindex="-1" class="playback-panel playback-topology">
              <div class="playback-panel-heading"><h3>哈希环</h3><span v-if="latestTopology" class="unit-tag">快照 #{{ latestTopology.line }}</span></div>
              <TopologyView :run="run" :run-id="live?.session?.id" :events="seenTopology" :load="nodeLoad" :owned-keys="nodeOwnedKeys"
                :has-request-samples="nodeSamples.length > 0" :next-request-sample="nextNodeSample" follow @jump-to-sample="jump" />
            </section>
          </template>
        </div>

        <div class="playback-records">
          <section v-if="caseStudy" id="playback-case" tabindex="-1" class="playback-section">
            <div class="section-heading"><h3>{{ stageRows.length ? '负载阶梯案例' : scenario === 'fault' ? '故障恢复案例' : scenario === 'migration' ? '扩缩容案例' : '拓扑与事件案例' }}</h3></div>
            <template v-if="stageRows.length">
              <p class="case-question">提高目标速率后，实际速率和尾延迟各变了多少？</p>
              <div class="table-scroll case-table"><table>
                <thead><tr><th scope="col">目标 ops/s</th><th scope="col">实际 ops/s</th><th scope="col">GET p99 µs</th><th scope="col">GET 样本</th></tr></thead>
                <tbody><tr v-for="[name, stage] in stageRows" :key="name"><th scope="row">{{ fmt(stage.target_rate) }}<span v-if="stage.sender_saturated" class="text-warning"> · 发送端饱和</span></th><td>{{ fmt(stage.achieved_rate, 2) }}</td><td>{{ fmt(stage.latency_us?.get?.p99, 2) }}</td><td>{{ fmt(stage.latency_us?.get?.samples) }}</td></tr></tbody>
              </table></div>
              <p class="footnote">阶段成绩来自 summary，只描述本次运行。</p>
            </template>
            <template v-else>
              <p class="case-question">{{ scenario === 'fault' ? '故障发生后，拓扑和副本用了多久恢复？' : '沿事件查看拓扑切换前后的节点归属。' }}</p>
              <dl v-if="recordedEpochs.length || scenarioFacts.length" class="sample-facts scenario-facts">
                <div v-if="recordedEpochs.length"><dt>记录的 epoch</dt><dd>{{ recordedEpochs.join(' → ') }}</dd></div>
                <div v-for="item in scenarioFacts" :key="item.label"><dt>{{ item.label }}</dt><dd>{{ fmt(item.value, 3) }} s</dd></div>
              </dl>
            </template>
            <div v-if="demonstration.length" class="case-steps">
              <button v-for="item in demonstration" :key="item.event.line" :aria-pressed="!showStages && currentEvent?.line === item.event.line" @click="view = 'metrics'; jump(item.event.line)">
                <strong>{{ item.label }}</strong><time>{{ relativeEventTime(item.event.unix_ms, bounds.origin) }}</time>
              </button>
            </div>
          </section>

          <section id="playback-current" tabindex="-1" class="playback-section">
            <div class="section-heading"><h3>{{ followLive ? '当前采样' : '游标读数' }}</h3><span v-if="latestSample" class="unit-tag">{{ relativeEventTime(latestSample.unix_ms, bounds.origin) }}</span></div>
            <p class="sample-context">{{ latestSample ? eventSummary(latestSample) : '到达采样点后显示读数。' }}</p>
            <dl class="replay-readings">
              <div v-for="metric in visibleMetrics" :key="metric">
                <dt>{{ operation.toUpperCase() }} {{ latencyCurveMetrics[metric].label }}</dt>
                <dd>{{ latestSample ? fmt(windowLatency(latestSample, operation, metric), 2) : '—' }} <small>µs</small></dd>
              </div>
              <div><dt>窗口成功速率</dt><dd>{{ latestSample ? fmt(windowSuccessRate(latestSample), 2) : '—' }} <small>ops/s</small></dd></div>
              <div><dt>目标速率</dt><dd>{{ fmt(latestSample?.fields.target_rate, 2) }} <small>ops/s</small></dd></div>
            </dl>
            <dl class="sample-facts">
              <div><dt>发出</dt><dd>{{ fmt(latestSample?.fields.issued) }}</dd></div>
              <div><dt>未按时发出</dt><dd>{{ fmt(latestSample?.fields.late) }}</dd></div>
              <div><dt>重试</dt><dd>{{ fmt(latestSample?.fields.retries) }}</dd></div>
              <div><dt>已发布 epoch</dt><dd>{{ latestTopology ? text(latestTopology.fields.epoch) : '—' }}</dd></div>
              <div class="sample-errors"><dt>窗口错误</dt><dd>{{ errorsText(latestSample?.fields.errors) }}</dd></div>
            </dl>
          </section>

          <section id="playback-events" tabindex="-1" class="playback-section">
            <ReplayEventList :events="seen" :total="log.events.length" :origin="bounds.origin" :selected="followLive ? inspectLine : currentEvent?.line"
              :animate="playing || followLive" :empty="followLive ? '等待首条事件。' : log.missing ? '未找到事件文件。' : log.events.length ? '等待回放到首条事件。' : '没有有效事件记录。'"
              @jump="jump($event.line)" />
          </section>

          <section id="playback-source" tabindex="-1" class="playback-section">
            <div class="section-heading"><div><h3>记录来源</h3><p v-if="inspectedEvent">#{{ inspectedEvent.line }} {{ eventName(inspectedEvent.event) }} · {{ absoluteEventTime(inspectedEvent.unix_ms) }}</p></div><button v-if="!live" class="table-link" @click="load">重新读取</button><button v-else-if="inspectLine !== null" class="table-link" @click="inspectLine = null">跟随最新</button></div>
            <details v-if="inspectedEvent" class="data-disclosure"><summary>查看事件字段</summary><pre class="source-view event-fields" tabindex="0" aria-label="当前事件字段">{{ JSON.stringify(inspectedEvent.fields, null, 2) }}</pre></details>
            <slot name="source" />
          </section>
        </div>
      </div>
    </template>
  </section>
</template>
