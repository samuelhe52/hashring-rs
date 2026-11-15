<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import DetailWindow from './components/DetailWindow.vue'
import FloatingIndex from './components/FloatingIndex.vue'
import ReferenceResults from './components/ReferenceResults.vue'
import RunCase from './components/RunCase.vue'
import RunCompare from './components/RunCompare.vue'
import RunConfig from './components/RunConfig.vue'
import RunExperiment from './components/RunExperiment.vue'
import RunFiles from './components/RunFiles.vue'
import RunGeneralOverview from './components/RunGeneralOverview.vue'
import RunOverview from './components/RunOverview.vue'
import RunRateOverview from './components/RunRateOverview.vue'
import RunReplay from './components/RunReplay.vue'
import RunTimeline from './components/RunTimeline.vue'
import RunTopology from './components/RunTopology.vue'
import { sceneFor, scenes } from './caseReplay'
import { canonical } from './compare.ts'
import { formatNumber as fmt, formatTime, modeLabels, number, object, request, stages, statusLabels, text } from './data'
import { absoluteEventTime, eventName } from './events'
import { directory, type DirectoryEntry, type Panel, type ReferenceView } from './navigation'
import type { RunDetail, RunEvent, RunListItem } from './types'

const runs = ref<RunListItem[]>([])
const run = ref<RunDetail | null>(null)
const selectedId = ref('')
const selectedStage = ref('')
const search = ref('')
const mode = ref('all')
const status = ref('all')
const source = ref('all')
const sort = ref('newest')
const listLoading = ref(true)
const detailLoading = ref(false)
const listError = ref('')
const detailError = ref('')
const unavailableId = ref('')
const panel = ref<Panel | null>(null)
const sourceEvent = ref<RunEvent>()
const snapshotLine = ref<number>()
const fileKind = ref<'raw' | 'logs'>('raw')
const referenceView = ref<ReferenceView>('load')
const views = reactive({ latency: 'curve', operation: 'get', nodes: 'owned_keys', observations: 'throughput', config: 'parameters', compare: 'latency', topology: 'range', case: 'stages', replay: 'metrics' })
const comparePlot = ref('bar')
const compareRepeats = ref(false)
const directoryAnchor = ref('')
let anchorObserver: MutationObserver | undefined
let listController: AbortController | undefined
let detailController: AbortController | undefined

const panelTitles: Record<Panel, string> = {
  runs: '切换运行', experiment: '运行实验', topology: '哈希环与拓扑', replay: '历史回放', case: '负载档位对比',
  compare: '跨运行对比', latency: '延迟与速率', nodes: '节点负载', events: '事件时间线', config: '实验配置',
  observations: '其他观测', files: '文件与日志', reference: '实验报告', event: '事件原文',
}
const sampleTitles: Record<string, string> = {
  [scenes.expansion.sample]: scenes.expansion.title,
  [scenes.fault.sample]: scenes.fault.title,
  'balance-v200': 'V=200 均衡复测',
}
const config = computed(() => object(run.value?.manifest?.config))
const scene = computed(() => sceneFor(config.value))
const rateStages = computed(() => stages(run.value?.summary ?? null))
const contextTitle = computed(() => run.value ? displayName(run.value) : '')
const modeOptions = computed(() => [...new Set(runs.value.map(item => item.config.mode).filter((value): value is string => typeof value === 'string'))].sort())
const filteredRuns = computed(() => {
  const query = search.value.trim().toLowerCase()
  return runs.value.filter(item => (
    (!query || [displayName(item), item.id, modeName(item.config.mode)].some(value => value.toLowerCase().includes(query))) &&
    (mode.value === 'all' || item.config.mode === mode.value) &&
    (status.value === 'all' || item.status === status.value) &&
    (source.value === 'all' || (item.source ?? 'local') === source.value)
  )).sort((a, b) => sort.value === 'name' ? displayName(a).localeCompare(displayName(b), 'zh-CN')
    : ((a.started_unix_ms ?? 0) - (b.started_unix_ms ?? 0)) * (sort.value === 'oldest' ? 1 : -1))
})
const runGroups = computed(() => [
  { title: '本地结果', sample: false, items: filteredRuns.value.filter(item => !item.source) },
  { title: '内置样例', sample: true, items: filteredRuns.value.filter(item => item.source === 'demo') },
].filter(group => group.items.length))
const pageIndex = computed(() => directory.map(group => ({
  ...group,
  items: group.items.filter(item => rateStages.value.length || !['latency', 'nodes', 'case'].includes(item.panel ?? '')),
})).filter(group => group.items.length))
const directoryActive = computed(() => {
  if (directoryAnchor.value) return directoryAnchor.value
  const parent = directory.flatMap(group => group.items).find(item => item.panel === panel.value)
  if (!parent) return 'overview'
  const view = panel.value === 'files' ? fileKind.value : panel.value === 'reference' ? referenceView.value : views[panel.value as keyof typeof views]
  return parent.children?.find(item => !item.anchor && item.view === view && (!item.plot || item.plot === comparePlot.value))?.id ?? parent.id
})
const modalTitle = computed(() => {
  if (panel.value === 'reference') return referenceView.value === 'acceptance' ? '课程验收对照' : '负载与均衡'
  if (panel.value === 'event' && sourceEvent.value) return eventName(sourceEvent.value.event)
  return panel.value ? panelTitles[panel.value] : ''
})
const modalSubtitle = computed(() => {
  if (panel.value === 'event' && sourceEvent.value) {
    return 'events.jsonl · 第 ' + sourceEvent.value.line + ' 行 · ' + absoluteEventTime(sourceEvent.value.unix_ms) + ' 北京时间'
  }
  if (panel.value === 'runs' || panel.value === 'experiment' || panel.value === 'compare' || panel.value === 'reference') return undefined
  return run.value?.name
})
const rawEvents = computed(() => '/api/runs/' + encodeURIComponent(selectedId.value) + '/raw?file=events.jsonl')
const compareSelection = computed(() => {
  const available = runs.value.filter(item => item.id !== selectedId.value && item.status === 'success' && item.name !== run.value?.name)
  const configKey = (value: RunListItem['config']) =>
    canonical(Object.fromEntries(Object.entries(value).filter(([field]) => field !== 'output_dir' && field !== 'write_ack_policy')))
  const match = available.find(item => configKey(item.config) === configKey(config.value))
  const second = match ?? (rateStages.value.length ? available.find(item => !item.source && item.config.mode === config.value.mode) : undefined)
  return [selectedId.value, second?.id].filter((id): id is string => !!id)
})
const sampleCommand = computed(() => {
  if (!scene.value || run.value?.name !== scenes[scene.value].sample) return ''
  const fault = scene.value === 'fault'
  return [
    'target/release/hashring-rs experiment --mode ' + (fault ? 'availability' : 'correctness'),
    '  --nodes 4 --keys ' + (fault ? '300' : '2000') + ' --virtual-nodes 32 --concurrency 8 --seed 1',
    '  --desired-replication-factor 3 --policy ' + (fault ? 'first-successor' : 'owner-only'),
    '  --worker-threads 2 --coordinator-worker-threads 2 --output results/sample-' + scene.value,
  ].join(' \\\n')
})

function displayName(item: RunDetail | RunListItem) {
  return item.source === 'demo' ? sampleTitles[item.name] ?? item.name : item.name
}
function modeName(value: unknown) {
  return modeLabels[text(value)] ?? text(value)
}
function openReference(view: ReferenceView) {
  referenceView.value = view
  panel.value = 'reference'
}
function openTopology(line?: number) {
  snapshotLine.value = line
  views.topology = 'range'
  panel.value = 'topology'
}
function inspectEvent(event: RunEvent) {
  sourceEvent.value = event
  panel.value = 'event'
}
function openObservations(view: 'throughput' | 'idle') {
  views.observations = view
  panel.value = 'observations'
}

async function navigate(entry: DirectoryEntry, keyboard: boolean) {
  anchorObserver?.disconnect()
  directoryAnchor.value = ''
  const previous = panel.value
  panel.value = entry.panel
  if (entry.panel === 'topology' && previous !== 'topology') snapshotLine.value = undefined
  if (entry.view && entry.panel === 'reference') referenceView.value = entry.view as ReferenceView
  else if (entry.view && entry.panel === 'files') fileKind.value = entry.view as 'raw' | 'logs'
  else if (entry.view && entry.panel && entry.panel in views) views[entry.panel as keyof typeof views] = entry.view
  if (entry.plot) comparePlot.value = entry.plot
  if (entry.id === 'compare-repeat') compareRepeats.value = true
  await nextTick()
  const behavior = keyboard || matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth'
  if (!entry.panel) {
    window.scrollTo({ top: 0, behavior })
    if (keyboard) {
      await nextTick()
      document.getElementById('overview')?.focus({ preventScroll: true })
    }
    return
  }
  const body = document.querySelector<HTMLElement>('.window-body')
  if (!body) return
  if (!entry.anchor) {
    if (previous === entry.panel) body.scrollTo({ top: 0, behavior: 'instant' })
    if (keyboard) document.getElementById('window-title')?.focus({ preventScroll: true })
    return
  }
  directoryAnchor.value = entry.id
  const go = () => {
    const section = entry.anchor ? document.getElementById(entry.anchor) : null
    if (!section || !body.contains(section)) return
    anchorObserver?.disconnect()
    if (entry.anchor === 'playback-source') section.querySelector('details')?.setAttribute('open', '')
    body.scrollTo({ top: body.scrollTop + section.getBoundingClientRect().top - body.getBoundingClientRect().top - 12, behavior })
    if (keyboard) section.focus({ preventScroll: true })
  }
  anchorObserver = new MutationObserver(go)
  anchorObserver.observe(body, { childList: true, subtree: true })
  go()
}

watch(panel, () => { anchorObserver?.disconnect(); directoryAnchor.value = '' })
watch(() => Object.values(views), () => { directoryAnchor.value = '' })

async function selectRun(id: string, refresh = false) {
  panel.value = null
  if (!refresh && run.value?.id === id) return
  detailController?.abort()
  detailController = new AbortController()
  const signal = detailController.signal
  selectedId.value = id
  selectedStage.value = ''
  views.operation = 'get'
  run.value = null
  detailError.value = ''
  detailLoading.value = true
  const url = new URL(location.href)
  url.search = new URLSearchParams({ run: id }).toString()
  history.replaceState(history.state, '', url)
  try {
    const data = await request<RunDetail>('/api/runs/' + encodeURIComponent(id), signal)
    if (signal.aborted) return
    run.value = data
    sessionStorage.setItem('hashring.dashboard.run', id)
    views.case = 'stages'
    views.observations = ['scaling', 'fault', 'availability', 'correctness'].includes(text(data.manifest?.config?.mode)) ? 'structured' : 'throughput'
  } catch (failure) {
    if (!signal.aborted) detailError.value = failure instanceof Error ? failure.message : String(failure)
  } finally {
    if (!signal.aborted) detailLoading.value = false
  }
}
async function loadRuns(id?: string) {
  listController?.abort()
  listController = new AbortController()
  const signal = listController.signal
  listLoading.value = true
  listError.value = ''
  try {
    const response = await request<{ runs: RunListItem[] }>('/api/runs?include=demo', signal)
    if (signal.aborted) return
    runs.value = response.runs
    const explicit = id ?? new URLSearchParams(location.search).get('run')
    const wanted = explicit || selectedId.value || sessionStorage.getItem('hashring.dashboard.run')
    const linked = runs.value.find(item => item.id === wanted) ?? runs.value.find(item => item.source === 'demo' && item.name === wanted)
    const next = linked ?? runs.value.find(item => !item.source) ??
      runs.value.find(item => item.name === scenes.fault.sample) ?? runs.value[0]
    unavailableId.value = explicit && !linked ? explicit : ''
    if (next) await selectRun(next.id, !!run.value)
    else {
      detailController?.abort()
      selectedId.value = ''
      run.value = null
      detailError.value = ''
      detailLoading.value = false
    }
  } catch (failure) {
    if (!signal.aborted) listError.value = failure instanceof Error ? failure.message : String(failure)
  } finally {
    if (!signal.aborted) listLoading.value = false
  }
}

function choose(id: string) {
  unavailableId.value = ''
  void selectRun(id)
}
onMounted(() => { void loadRuns() })
onBeforeUnmount(() => { listController?.abort(); detailController?.abort(); anchorObserver?.disconnect() })
</script>

<template>
  <div class="dashboard-shell">
    <header class="dashboard-header">
      <div class="dashboard-title">
        <svg class="brand-mark" viewBox="0 0 36 36" fill="none" aria-hidden="true"><circle cx="18" cy="18" r="12" stroke="currentColor" stroke-width="1.2"/><circle cx="18" cy="6" r="3" fill="var(--accent)"/><circle cx="28.4" cy="24" r="2.6" fill="var(--teal)"/><circle cx="7.6" cy="24" r="2.6" fill="var(--gold)"/><circle cx="18" cy="18" r="2" fill="currentColor"/></svg>
        <h1>hashring-rs</h1><span>实验 Dashboard</span>
      </div>
      <div class="dashboard-actions">
        <button class="button secondary dashboard-picker" aria-haspopup="dialog" @click="panel = 'runs'">切换运行 <span aria-hidden="true">⌄</span></button>
        <button class="button icon-button" :disabled="listLoading" aria-label="刷新结果" @click="loadRuns()"><svg viewBox="0 0 20 20" aria-hidden="true"><path d="M16 7a6.5 6.5 0 1 0 .4 5M16 3v4h-4" /></svg></button>
        <button class="button primary" aria-haspopup="dialog" @click="panel = 'experiment'">运行实验</button>
        <div class="dashboard-index"><FloatingIndex :groups="pageIndex" active="overview" :has-run="!!run" @navigate="navigate" /></div>
      </div>
    </header>

    <main class="dashboard-main">
      <section id="overview" aria-label="运行概览" tabindex="-1" :aria-busy="detailLoading">
        <div v-if="listError && run" class="notice error" role="alert"><p>{{ listError }}</p><button class="table-link" @click="loadRuns()">重试读取</button></div>
        <div v-if="detailLoading || (listLoading && !run)" class="dashboard-empty" role="status"><span class="loading-dot"></span><h2>正在读取运行结果</h2></div>
        <div v-else-if="listError && !run" class="dashboard-empty" role="alert"><h2>结果列表读取失败</h2><p>{{ listError }}</p><button class="button secondary" @click="loadRuns()">重试读取</button></div>
        <div v-else-if="detailError" class="dashboard-empty" role="alert"><h2>运行结果读取失败</h2><p>{{ detailError }}</p><button class="button secondary" @click="selectRun(selectedId, true)">重新读取</button></div>
        <template v-else-if="run">
          <div class="dashboard-context">
            <div class="dashboard-context-name">
              <span class="mode-label">{{ modeName(config.mode) }}</span>
              <h2 :title="run.name">{{ contextTitle }}</h2>
              <span class="status-badge" :class="run.status"><i></i>{{ statusLabels[run.status] }}</span>
            </div>
            <div class="dashboard-context-meta">
              <span v-if="run.source === 'demo'" class="dashboard-source-tag">内置实录</span>
              <time v-else-if="number(run.manifest?.started_unix_ms) !== null">{{ formatTime(run.manifest?.started_unix_ms) }}</time>
              <button v-if="run.manifest?.source_reproducible === false || run.warnings.length" class="context-warning" aria-haspopup="dialog" @click="panel = 'config'">{{ run.manifest?.source_reproducible === false ? '源码可复现性待核对' : '文件读取提示' }}</button>
              <button class="table-link" aria-haspopup="dialog" @click="panel = 'replay'">{{ scene ? '完整回放' : '历史回放' }} ↗</button>
            </div>
          </div>
          <p v-if="unavailableId" class="notice warning">未找到运行 {{ unavailableId }}。已打开可用记录。</p>
          <div v-if="run.status === 'failed' || run.summary?.error" class="notice error" role="alert"><p><strong>实验失败</strong><span>{{ run.summary?.error || '未记录失败原因。' }}</span></p></div>
          <p v-else-if="run.status === 'incomplete'" class="notice neutral">运行结果尚不完整，已记录的数据仍可查看。</p>

          <RunCase v-if="scene" :key="run.id" :run="run" :scene="scene" :modal-open="!!panel" @inspect="inspectEvent" @topology="openTopology" />
          <RunRateOverview v-else-if="rateStages.length" :key="run.id" :run="run" :rows="rateStages"
            v-model:stage="selectedStage" v-model:load-metric="views.nodes" v-model:operation="views.operation" @details="panel = $event" />
          <RunGeneralOverview v-else :key="run.id" :run="run" @observations="openObservations" />

          <nav class="dashboard-resources" aria-label="运行分析">
            <button v-if="rateStages.length" aria-haspopup="dialog" @click="panel = 'case'"><span>负载档位对比</span><span aria-hidden="true">↗</span></button>
            <button v-else aria-haspopup="dialog" @click="panel = 'observations'"><span>其他观测</span><span aria-hidden="true">↗</span></button>
            <button aria-haspopup="dialog" @click="openTopology()"><span>哈希环与拓扑</span><span aria-hidden="true">↗</span></button>
            <button aria-haspopup="dialog" @click="panel = 'compare'"><span>跨运行对比</span><span aria-hidden="true">↗</span></button>
            <button aria-haspopup="dialog" @click="panel = 'config'"><span>实验配置</span><span aria-hidden="true">↗</span></button>
          </nav>
          <nav class="dashboard-source-links" aria-label="记录与报告">
            <button class="table-link" aria-haspopup="dialog" @click="panel = 'events'">事件时间线</button>
            <button class="table-link" aria-haspopup="dialog" @click="fileKind = 'raw'; panel = 'files'">文件与日志</button>
            <button class="table-link" aria-haspopup="dialog" @click="openReference('load')">正式负载曲线</button>
            <button class="table-link" aria-haspopup="dialog" @click="openReference('acceptance')">验收对照</button>
          </nav>
        </template>
        <div v-else class="dashboard-empty">
          <span class="empty-ring" aria-hidden="true"></span><h2>从一次实验开始</h2><p>将历史记录放入 results，或启动一次实验。</p><button class="button secondary" @click="loadRuns()">刷新结果</button>
        </div>
      </section>
    </main>

    <DetailWindow v-if="panel" :page-key="panel" :title="modalTitle" :subtitle="modalSubtitle"
      :compact="panel === 'runs' || panel === 'event'" :wide="['replay', 'case', 'experiment', 'compare'].includes(panel)" @close="panel = null">
      <template #navigation><FloatingIndex :groups="pageIndex" :active="directoryActive" :has-run="!!run" @navigate="navigate" /></template>

      <div v-if="panel === 'runs'" class="run-picker dashboard-run-picker">
        <label class="search-field"><svg viewBox="0 0 20 20" aria-hidden="true"><circle cx="8.5" cy="8.5" r="5.5"/><path d="m13 13 4 4"/></svg><input v-model="search" type="search" placeholder="搜索运行名称" aria-label="搜索运行名称" /></label>
        <div class="filter-row">
          <select v-model="mode" aria-label="筛选实验模式"><option value="all">全部模式</option><option v-for="item in modeOptions" :key="item" :value="item">{{ modeName(item) }}</option></select>
          <select v-model="status" aria-label="筛选运行状态"><option value="all">全部状态</option><option value="success">成功</option><option value="failed">失败</option><option value="incomplete">未完成</option></select>
          <select v-model="source" aria-label="筛选运行来源"><option value="all">全部来源</option><option value="local">本地结果</option><option value="demo">内置样例</option></select>
        </div>
        <div class="list-sort"><span>{{ filteredRuns.length }} 次运行</span><select v-model="sort" aria-label="运行排序"><option value="newest">最新优先</option><option value="oldest">最早优先</option><option value="name">名称排序</option></select></div>
        <p v-if="listLoading" class="empty-inline" role="status">正在读取运行列表</p>
        <div v-else-if="listError" class="notice error" role="alert"><p>{{ listError }}</p><button class="table-link" @click="loadRuns()">重试读取</button></div>
        <div v-else-if="!filteredRuns.length" class="sidebar-empty">
          <strong>{{ runs.length ? '没有匹配的运行' : '还没有实验结果' }}</strong>
          <button v-if="runs.length" class="table-link" @click="search = ''; mode = 'all'; status = 'all'; source = 'all'">清除筛选</button>
        </div>
        <section v-for="group in runGroups" v-else :key="group.title" class="dashboard-run-group" :class="{ 'bundled-runs': group.sample }">
          <h3>{{ group.title }}</h3>
          <ul class="run-list">
            <li v-for="item in group.items" :key="item.id">
              <button class="run-card" :class="{ active: item.id === selectedId }" :aria-pressed="item.id === selectedId" @click="choose(item.id)">
                <span class="run-card-top"><span class="mode-label">{{ modeName(item.config.mode) }}</span><span class="status-badge" :class="item.status"><i></i>{{ statusLabels[item.status] }}</span></span>
                <strong class="run-name">{{ displayName(item) }}</strong>
                <span class="run-date">{{ formatTime(item.started_unix_ms) }}</span>
                <span class="run-card-meta"><span>{{ fmt(item.config.key_count) }} 键</span><span>{{ fmt(item.config.node_count) }} 节点</span><span>RF {{ fmt(item.config.desired_replication_factor) }}</span><span>ACK {{ text(item.config.write_ack_policy) }}</span><span>{{ fmt(item.elapsed_seconds, 2) }} s</span></span>
                <span v-if="item.warnings.length" class="text-warning">部分文件读取异常</span>
              </button>
            </li>
          </ul>
        </section>
        <p v-if="filteredRuns.length" class="sidebar-footnote">开始时间均为北京时间。</p>
      </div>
      <RunExperiment v-else-if="panel === 'experiment'" @result="loadRuns" />
      <ReferenceResults v-else-if="panel === 'reference'" :view="referenceView" @view="referenceView = $event" />
      <RunCompare v-else-if="panel === 'compare'" v-model:view="views.compare" v-model:plot="comparePlot" v-model:group-repeats="compareRepeats"
        :runs="runs" :initial-id="selectedId" :initial-selection="compareSelection" />
      <section v-else-if="panel === 'event' && sourceEvent" class="dashboard-event-source">
        <p><a :href="rawEvents" class="button secondary" target="_blank" rel="noopener">打开完整 events.jsonl ↗</a></p>
        <h3>事件字段</h3><pre class="source-view event-fields" tabindex="0" aria-label="事件字段">{{ JSON.stringify(sourceEvent.fields, null, 2) }}</pre>
        <details class="data-disclosure"><summary>JSONL 本行原文</summary><pre class="source-view" tabindex="0">{{ sourceEvent.raw }}</pre></details>
      </section>
      <template v-else-if="run">
        <RunReplay v-if="panel === 'replay' || panel === 'case'" v-model:view="views[panel]" :run="run" :case-study="panel === 'case'" />
        <RunTopology v-else-if="panel === 'topology'" v-model:view="views.topology" :run="run" :initial-line="snapshotLine" />
        <RunOverview v-else-if="panel === 'latency' || panel === 'nodes' || panel === 'observations'"
          v-model:view="views.latency" v-model:observation-view="views.observations" v-model:load-metric="views.nodes" v-model:operation="views.operation"
          :run="run" :section="panel" :stage="selectedStage" @select-stage="selectedStage = $event" />
        <RunTimeline v-else-if="panel === 'events'" :run="run" />
        <template v-else-if="panel === 'config'">
          <details v-if="sampleCommand" class="data-disclosure dashboard-replay-command"><summary>样例复跑命令</summary><pre class="source-view" tabindex="0">{{ sampleCommand }}</pre></details>
          <RunConfig v-model:view="views.config" :run="run" />
        </template>
        <template v-else-if="panel === 'files'">
          <nav class="view-switch" aria-label="文件内容"><button :aria-pressed="fileKind === 'raw'" @click="fileKind = 'raw'">原始结果</button><button :aria-pressed="fileKind === 'logs'" @click="fileKind = 'logs'">进程日志</button></nav>
          <RunFiles :key="fileKind" :run-id="run.id" :kind="fileKind" />
        </template>
      </template>
    </DetailWindow>
  </div>
</template>
