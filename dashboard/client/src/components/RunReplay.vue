<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import DataChart from './DataChart.vue'
import ReplayEventList from './ReplayEventList.vue'
import StageComparison from './StageComparison.vue'
import TopologyView from './TopologyView.vue'
import type { EventLog, ExperimentFeed, RunDetail, RunEvent } from '../types'
import { colors, errorsText, formatNumber as fmt, number, object, operations, request, stages, text } from '../data'
import { absoluteEventTime, eventCategory, eventName, eventSummary, isKeyEvent, relativeEventTime } from '../events'
import { replayBounds, sampledSeries, visibleCount, warmupIntervals, windowLatency, windowSuccessRate } from '../history.ts'
import { useReplayClock } from '../useReplayClock'

const props = defineProps<{ run?: RunDetail; live?: ExperimentFeed; caseStudy?: boolean }>()
const history = ref<EventLog>({ events: [], issues: [], missing: false })
const log = computed(() => props.live?.log ?? history.value)
const loading = ref(!props.live)
const error = ref('')
const hideWarmup = ref(false)
const markers = ref(true)
const plot = ref('latency')
const stageRows = computed(() => stages(props.run?.summary ?? null))
const view = defineModel<string>('view', { default: 'metrics' })
const operation = ref('get')
const percentile = ref('p99')
const inspectLine = ref<number | null>(null)
const chart = ref<InstanceType<typeof DataChart> | null>(null)
const layoutVersion = ref(0)
let controller: AbortController | undefined
const bounds = computed(() => replayBounds(log.value.events, props.run?.manifest?.started_unix_ms ?? props.live?.session?.startedAt))
const { playing, speed, cursor, boundaryLine, pause, seek, toggle } = useReplayClock(() => bounds.value)
const time = computed(() => props.live ? bounds.value.end : cursor.value)
const count = computed(() => visibleCount(log.value.events, time.value, boundaryLine.value ?? Infinity))
const seen = computed(() => log.value.events.slice(0, count.value))
const currentEvent = computed(() => seen.value.at(-1))
const inspectedEvent = computed(() => log.value.events.find(event => event.line === inspectLine.value) ?? currentEvent.value)
const allSamples = computed(() => log.value.events.filter(event => event.event === 'metrics_sampled'))
const sampleCount = computed(() => visibleCount(allSamples.value, time.value, boundaryLine.value ?? Infinity))
const samples = computed(() => allSamples.value.slice(0, sampleCount.value).filter(event => !hideWarmup.value || event.fields.warmup !== true))
const latestSample = computed(() => samples.value.at(-1))
const topologyRecords = computed(() => log.value.events.filter(event => event.event === 'topology_published'))
const topologyCount = computed(() => visibleCount(topologyRecords.value, time.value, boundaryLine.value ?? Infinity))
const seenTopology = computed(() => topologyRecords.value.slice(0, topologyCount.value))
const latestTopology = computed(() => seenTopology.value.at(-1))
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
  const list: { event: RunEvent; label: string; view: string }[] = []
  const ring = topologyRecords.value[0]
  if (ring) list.push({ event: ring, label: '初始哈希环', view: 'topology' })
  const firstOfStage = new Map<unknown, RunEvent>()
  for (const event of allSamples.value) {
    if (event.fields.target_rate !== undefined && event.fields.warmup !== true && !firstOfStage.has(event.fields.stage)) firstOfStage.set(event.fields.stage, event)
  }
  for (const event of firstOfStage.values()) list.push({ event, label: `${fmt(event.fields.target_rate)} ops/s`, view: 'metrics' })
  for (const event of keyEvents.value) {
    if (['migration_started', 'migration_complete', 'scaling_transition_started', 'scaling_transition_complete', 'owner_killed', 'failover_published', 'full_rf_restored', 'fault_injected', 'fault_published', 'fault_full_rf_restored'].includes(event.event)) {
      list.push({ event, label: eventName(event.event), view: 'topology' })
    }
  }
  return list.sort((a, b) => a.event.unix_ms - b.event.unix_ms || a.event.line - b.event.line)
})
const seriesDefs = computed<{ name: string; value: (event: RunEvent) => number | null }[]>(() => {
  if (plot.value === 'latency') return [{ name: `${operation.value.toUpperCase()} ${percentile.value}`, value: event => windowLatency(event, operation.value, percentile.value) }]
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
  color: colors, grid: { top: 28, left: 50, right: 22, bottom: 45, outerBoundsMode: 'same' },
  tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => `${fmt(Array.isArray(value) ? value.at(-1) : value, 2)} ${unit.value}` },
  xAxis: { type: 'value', min: 0, max: props.live ? Math.max(10, Math.ceil(duration.value / 10000) * 10) : Math.max(1, duration.value / 1000), name: '记录时间（s）', nameLocation: 'middle', nameGap: 30 },
  yAxis: { type: 'value', min: 0, name: unit.value, splitLine: { lineStyle: { color: '#e5e0d7', type: 'dashed' } } },
  series: seriesDefs.value.map((series, index) => ({ name: series.name, type: 'line', symbol: 'circle', symbolSize: 5, smooth: false, connectNulls: false, lineStyle: { width: 2, type: series.name === '目标速率' ? 'dashed' : 'solid' },
    data: sampledSeries(seen.value, bounds.value.start, series.value, hideWarmup.value),
    ...(index === 0 ? {
      markArea: { silent: true, itemStyle: { color: '#ccc6bb40' }, data: warmupIntervals(seen.value, bounds.value.start).map(([from, to]) => [{ xAxis: from }, { xAxis: to }]) },
      markLine: { silent: true, symbol: ['none', 'none'], label: { show: false }, lineStyle: { color: '#736f65', opacity: .35, type: 'dashed' }, data: markers.value ? seen.value.filter(event => ['topology', 'migration', 'fault'].includes(eventCategory(event.event))).map(event => ({ name: eventName(event.event), xAxis: (event.unix_ms - bounds.value.start) / 1000 })) : [] },
    } : {}),
  })),
}))
const hasPlottedValue = computed(() => seriesDefs.value.some(series => samples.value.some(event => series.value(event) !== null)))

function jump(line: number) {
  if (props.live) { inspectLine.value = line; return }
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
    if (!signal.aborted) {
      seek(props.caseStudy ? bounds.value.end : bounds.value.start)
      if (props.caseStudy && !stageRows.value.length && topologyRecords.value.length) view.value = 'topology'
    }
  }
  catch (failure) { if (!signal.aborted) error.value = failure instanceof Error ? failure.message : String(failure) }
  finally { if (!signal.aborted) loading.value = false }
}
onMounted(() => { if (!props.live) void load() })
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
          <nav class="view-switch" :aria-label="live ? '实时视图' : '回放视图'">
            <button v-if="stageRows.length" :aria-pressed="view === 'stages'" @click="view = 'stages'">阶段对比</button>
            <button :aria-pressed="view === 'metrics'" @click="view = 'metrics'">指标曲线</button>
            <button :aria-pressed="view === 'topology'" @click="view = 'topology'">哈希环</button>
          </nav>

          <StageComparison v-if="view === 'stages' && run" :run="run" />
          <TopologyView v-else-if="view === 'topology'" :run="run" :events="seenTopology" follow />
          <div v-else-if="allSamples.length" class="playback-plot">
            <div class="comparison-controls">
              <select v-model="plot" :aria-label="live ? '实时图表' : '回放图表'"><option value="latency">窗口延迟</option><option value="rate">目标与实际速率</option><option value="counts">发送与重试</option><option value="errors">错误类别</option></select>
              <template v-if="plot === 'latency'">
                <select v-model="operation" aria-label="回放操作"><option v-for="op in plottedOperations" :key="op" :value="op">{{ op.toUpperCase() }}</option></select>
                <select v-model="percentile" aria-label="回放分位值"><option v-for="key in ['p50', 'p95', 'p99', 'max']" :key="key">{{ key }}</option></select>
              </template>
              <label class="checkbox-label"><input v-model="hideWarmup" type="checkbox" />隐藏预热</label>
              <label class="checkbox-label"><input v-model="markers" type="checkbox" />事件标记</label>
            </div>
            <div class="replay-legend"><span v-for="(series, index) in seriesDefs" :key="series.name"><i :style="{ background: colors[index % colors.length] }"></i>{{ series.name }}</span></div>
            <div class="replay-chart">
              <DataChart ref="chart" :option="chartOption" :motion="false" :label="(live ? '实时' : '回放') + '窗口记录曲线'" @layout="layoutVersion++" />
              <div v-if="playhead !== null" class="chart-playhead" :style="{ transform: 'translateX(' + playhead + 'px)' }" aria-hidden="true"></div>
              <p v-if="!hasPlottedValue" class="chart-empty-label">{{ samples.length ? '所选指标没有记录' : '等待首个采样窗口' }}</p>
            </div>
            <p class="footnote">灰底标记预热，虚线标记拓扑、迁移和故障事件。曲线按采样窗口绘制，缺失数据处断开。</p>
          </div>
          <div v-else class="playback-wait">
            <span class="empty-ring" aria-hidden="true"></span>
            <p>{{ live && !live.session?.endedAt ? '等待指标采样' : '本次运行没有指标采样' }}</p>
            <small>事件和拓扑会按实际记录显示。</small>
          </div>

          <div v-if="!live && log.events.length && view !== 'stages'" class="playback-transport">
            <div class="replay-position"><span>{{ fmt(seconds, 2) }} / {{ fmt(duration / 1000, 2) }} s</span><span>{{ count }} / {{ log.events.length }} 条</span></div>
            <input class="replay-seek" type="range" aria-label="回放进度" :aria-valuetext="relativeEventTime(cursor, bounds.start)" :min="bounds.start" :max="bounds.end" :value="cursor" :disabled="!duration" :style="{ '--playback-progress': (progress * 100) + '%' }" step="1" @input="seek(Number(($event.target as HTMLInputElement).value))" />
            <div class="replay-controls">
              <button class="button primary" :disabled="!duration" @click="toggle">{{ playing ? '暂停' : cursor >= bounds.end ? '从头播放' : '播放' }}</button>
              <button class="button secondary" aria-label="上一个事件" :disabled="count <= 1" @click="step(-1)">←</button>
              <button class="button secondary" aria-label="下一个事件" :disabled="count >= log.events.length" @click="step(1)">→</button>
              <label>速度<select v-model="speed" aria-label="回放速度"><option v-for="value in [1, 2, 5, 10]" :key="value" :value="value">{{ value }}×</option></select></label>
              <button class="table-link" @click="seek(bounds.start)">起点</button>
              <button class="table-link" @click="seek(bounds.end)">结束</button>
            </div>
          </div>
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
              <button v-for="item in demonstration" :key="item.event.line" :aria-pressed="currentEvent?.line === item.event.line && view === item.view" @click="view = item.view; jump(item.event.line)">
                <strong>{{ item.label }}</strong><time>{{ relativeEventTime(item.event.unix_ms, bounds.origin) }}</time>
              </button>
            </div>
          </section>

          <section id="playback-current" tabindex="-1" class="playback-section">
            <div class="section-heading"><div><h3>{{ live ? '当前采样' : '游标读数' }}</h3><p>{{ latestSample ? eventSummary(latestSample) : '到达采样点后显示读数。' }}</p></div><span class="unit-tag">{{ fmt(seconds, 2) }} s</span></div>
            <div class="replay-readings">
              <div><span>目标速率</span><strong>{{ fmt(latestSample?.fields.target_rate, 2) }}<small>ops/s</small></strong></div>
              <div><span>窗口成功速率</span><strong>{{ latestSample ? fmt(windowSuccessRate(latestSample), 2) : '—' }}<small>ops/s</small></strong></div>
              <div><span>{{ operation.toUpperCase() }} {{ percentile }}</span><strong>{{ latestSample ? fmt(windowLatency(latestSample, operation, percentile), 2) : '—' }}<small>µs</small></strong></div>
            </div>
            <dl class="sample-facts">
              <div><dt>发出</dt><dd>{{ fmt(latestSample?.fields.issued) }}</dd></div>
              <div><dt>未按时发出</dt><dd>{{ fmt(latestSample?.fields.late) }}</dd></div>
              <div><dt>重试</dt><dd>{{ fmt(latestSample?.fields.retries) }}</dd></div>
              <div><dt>已发布 epoch</dt><dd>{{ latestTopology ? text(latestTopology.fields.epoch) : '—' }}</dd></div>
              <div class="sample-errors"><dt>窗口错误</dt><dd>{{ errorsText(latestSample?.fields.errors) }}</dd></div>
            </dl>
          </section>

          <section id="playback-events" tabindex="-1" class="playback-section">
            <ReplayEventList :events="seen" :total="log.events.length" :origin="bounds.origin" :selected="live ? inspectLine : currentEvent?.line"
              :animate="playing || !!live" :empty="live ? '等待首条事件。' : log.missing ? '未找到事件文件。' : log.events.length ? '等待回放到首条事件。' : '没有有效事件记录。'"
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
