<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import type { EventLog, RunDetail } from '../types'
import { number, request } from '../data'
import { absoluteEventTime, categories, eventCategory, eventName, eventStage, eventSummary, isKeyEvent, relativeEventTime } from '../events'

const props = defineProps<{ run: RunDetail }>()
const log = ref<EventLog>({ events: [], issues: [], missing: false })
const loading = ref(true)
const error = ref('')
const search = ref('')
const category = ref('all')
const stage = ref('all')
const keyOnly = ref(true)
const hideWarmup = ref(false)
const timeMode = ref('relative')
const selectedLine = ref<number | null>(null)
const detailMode = ref('fields')
const page = ref(1)
const pageSize = 50
const eventList = ref<HTMLOListElement | null>(null)
const detailPanel = ref<HTMLElement | null>(null)
let controller: AbortController | undefined

const stageNames = computed(() => [...new Set(log.value.events.map(eventStage).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'en', { numeric: true })))
const categoryOptions = computed(() => Object.entries(categories).map(([value, label]) => ({
  value, label, count: log.value.events.filter(event => eventCategory(event.event) === value).length,
})).filter(option => option.count))
const keyEvents = computed(() => log.value.events.filter(isKeyEvent))
const sampledCount = computed(() => log.value.events.filter(event => event.event === 'metrics_sampled').length)
const topologyCount = computed(() => log.value.events.filter(event => event.event === 'topology_published').length)
const origin = computed(() => {
  const start = log.value.events.find(event => event.event === 'experiment_started')
  if (start) return { ms: start.unix_ms, label: '实验启动' }
  const manifestStart = number(props.run.manifest?.started_unix_ms)
  if (manifestStart !== null) return { ms: manifestStart, label: '清单开始时间' }
  return { ms: log.value.events[0]?.unix_ms ?? null, label: '首条有效事件' }
})
const filtered = computed(() => {
  const query = search.value.trim().toLowerCase()
  return log.value.events.filter(event =>
    (category.value === 'all' || eventCategory(event.event) === category.value) &&
    (stage.value === 'all' || eventStage(event) === stage.value) &&
    (!keyOnly.value || isKeyEvent(event)) &&
    (!hideWarmup.value || !(event.event === 'metrics_sampled' && event.fields.warmup === true)) &&
    (!query || `${eventName(event.event)} ${eventSummary(event)} ${event.raw}`.toLowerCase().includes(query)))
})
const pageCount = computed(() => Math.max(1, Math.ceil(filtered.value.length / pageSize)))
const pageEvents = computed(() => filtered.value.slice((page.value - 1) * pageSize, page.value * pageSize))
const selected = computed(() => log.value.events.find(event => event.line === selectedLine.value))
const rawUrl = computed(() => `/api/runs/${encodeURIComponent(props.run.id)}/raw?file=events.jsonl`)

watch([search, category, stage, keyOnly, hideWarmup], () => { page.value = 1 })
watch(pageEvents, events => {
  if (!events.some(event => event.line === selectedLine.value)) selectedLine.value = events[0]?.line ?? null
})
watch(selectedLine, () => { detailMode.value = 'fields' })
watch(page, () => { if (eventList.value) eventList.value.scrollTop = 0 })

function clearFilters() {
  search.value = ''
  category.value = 'all'
  stage.value = 'all'
  keyOnly.value = false
  hideWarmup.value = false
}

async function showDetail(line: number) {
  selectedLine.value = line
  await nextTick()
  if (matchMedia('(max-width: 800px)').matches) detailPanel.value?.scrollIntoView({ block: 'start' })
}

async function jumpTo(line: number) {
  if (!line) return
  clearFilters()
  await nextTick()
  const index = filtered.value.findIndex(event => event.line === line)
  if (index < 0) return
  page.value = Math.floor(index / pageSize) + 1
  selectedLine.value = line
  await nextTick()
  eventList.value?.querySelector(`[data-event-line="${line}"]`)?.scrollIntoView({ block: 'nearest' })
  await showDetail(line)
}

async function load() {
  controller?.abort()
  controller = new AbortController()
  const signal = controller.signal
  loading.value = true
  error.value = ''
  try {
    log.value = await request<EventLog>(`/api/runs/${encodeURIComponent(props.run.id)}/events`, signal)
    page.value = 1
  } catch (failure) {
    if (!signal.aborted) error.value = failure instanceof Error ? failure.message : String(failure)
  } finally { if (!signal.aborted) loading.value = false }
}
onMounted(load)
onBeforeUnmount(() => controller?.abort())
</script>

<template>
  <section class="timeline-view">
    <div class="section-heading"><div><h3>这一次，发生了什么？</h3><p>先看关键步骤，按需展开全部采样。</p></div><button class="button secondary" :disabled="loading" @click="load">{{ loading ? '正在读取' : '刷新事件' }}</button></div>
    <p v-if="loading" class="empty-inline" role="status">正在读取事件记录……</p>
    <div v-else-if="error" class="notice error" role="alert"><p>{{ error }}</p><button class="table-link" @click="load">重试读取</button></div>
    <template v-else>
      <details v-if="log.issues.length" class="notice warning event-issues">
        <summary>{{ log.issues.length }} 行事件无法解析，其他可读记录已保留</summary>
        <ul><li v-for="issue in log.issues" :key="issue.line"><strong>原文第 {{ issue.line }} 行</strong><p>{{ issue.message }}</p><pre>{{ issue.raw }}</pre></li></ul>
      </details>
      <div v-if="log.missing" class="empty-block"><h3>尚无事件文件</h3><p>此运行没有 events.jsonl。已有配置和最终成绩仍可在其他视图查看。</p></div>
      <div v-else-if="!log.events.length" class="empty-block"><h3>暂无可展示的事件</h3><p>{{ log.issues.length ? '请查看上方解析提示，或打开原文核对。' : '事件文件为空，之后可刷新重新读取。' }}</p><a :href="rawUrl" target="_blank" rel="noopener" class="table-link">打开事件原文 ↗</a></div>
      <template v-else>
        <div class="event-overview"><p><strong>{{ log.events.length }}</strong> 条事件<span>{{ sampledCount }} 个指标窗口</span><span>{{ topologyCount }} 份拓扑快照</span></p><a :href="rawUrl" target="_blank" rel="noopener">完整原文 ↗</a></div>
        <div class="timeline-quick"><label class="checkbox-label"><input v-model="keyOnly" type="checkbox" />只看关键事件</label><p>取消勾选可查看全部采样窗口。</p></div>
        <details class="data-disclosure event-filter-disclosure"><summary>搜索、筛选与跳转</summary>
        <div class="timeline-controls">
          <label class="event-search"><span class="sr-only">搜索事件</span><input v-model="search" type="search" aria-label="搜索事件" placeholder="搜索事件名、阶段或字段值" /></label>
          <select v-model="category" aria-label="筛选事件类别"><option value="all">全部类别</option><option v-for="option in categoryOptions" :key="option.value" :value="option.value">{{ option.label }}（{{ option.count }}）</option></select>
          <select v-model="stage" aria-label="筛选事件阶段"><option value="all">全部阶段</option><option v-for="name in stageNames" :key="name" :value="name">{{ name }}</option></select>
          <label class="checkbox-label"><input v-model="hideWarmup" type="checkbox" />隐藏预热采样</label>
        </div>
        <div class="timeline-tools"><span role="status">显示 {{ filtered.length }} / {{ log.events.length }} 条</span><label>时间<select v-model="timeMode" aria-label="时间显示方式"><option value="relative">相对{{ origin.label }}</option><option value="absolute">北京时间</option></select></label>
          <label class="event-jump">跳转<select :value="selected && isKeyEvent(selected) ? selected.line : ''" aria-label="跳转到关键事件" @change="jumpTo(Number(($event.target as HTMLSelectElement).value))"><option value="" disabled>选择关键事件</option><option v-for="event in keyEvents" :key="event.line" :value="event.line">{{ relativeEventTime(event.unix_ms, origin.ms) }} · {{ eventName(event.event) }}（第 {{ event.line }} 行）</option></select></label>
        </div>

        </details>
        <p class="event-visible-count" role="status">当前显示 {{ filtered.length }} / {{ log.events.length }} 条</p>
        <div v-if="!filtered.length" class="empty-block"><h3>没有匹配的事件</h3><p>可以调整关键词、类别或阶段。</p><button class="button secondary" @click="clearFilters">清除事件筛选</button></div>
        <div v-else class="timeline-browser">
          <div class="event-list-panel">
            <ol ref="eventList" class="event-list" aria-label="按时间排列的事件">
              <li v-for="event in pageEvents" :key="event.line">
                <button class="timeline-row" :class="[eventCategory(event.event), { selected: selectedLine === event.line, 'is-warmup': event.fields.warmup === true, 'is-failure': event.event.includes('failed') || event.fields.success === false }]" :data-event-line="event.line" :aria-pressed="selectedLine === event.line" :aria-label="`${eventName(event.event)}，原文第 ${event.line} 行`" @click="showDetail(event.line)">
                  <time :title="absoluteEventTime(event.unix_ms)">{{ timeMode === 'relative' ? relativeEventTime(event.unix_ms, origin.ms) : absoluteEventTime(event.unix_ms).split(' ').at(-1) }}</time>
                  <span class="event-dot" aria-hidden="true"></span>
                  <span class="event-copy"><span class="event-title">{{ eventName(event.event) }}<span v-if="event.fields.warmup === true" class="event-warmup">预热</span></span><span v-if="eventSummary(event)" class="event-description">{{ eventSummary(event) }}</span></span>
                  <span class="event-source-line">#{{ event.line }}</span>
                </button>
              </li>
            </ol>
            <div v-if="pageCount > 1" class="event-pagination"><button class="button secondary" :disabled="page === 1" @click="page--">上一页</button><span>第 {{ page }} / {{ pageCount }} 页</span><button class="button secondary" :disabled="page === pageCount" @click="page++">下一页</button></div>
          </div>
          <section v-if="selected" ref="detailPanel" class="event-detail" aria-label="事件详情">
            <div :key="selected.line" class="event-detail-heading"><span class="unit-tag">原文第 {{ selected.line }} 行</span><span class="event-category-label">{{ categories[eventCategory(selected.event)] }}</span><h4>{{ eventName(selected.event) }}</h4><code>{{ selected.event }}</code></div>
            <dl class="event-detail-time"><div><dt>北京时间</dt><dd>{{ absoluteEventTime(selected.unix_ms) }}</dd></div><div><dt>相对{{ origin.label }}</dt><dd>{{ relativeEventTime(selected.unix_ms, origin.ms) }}</dd></div></dl>
            <div class="segmented" aria-label="事件详情内容"><button :aria-pressed="detailMode === 'fields'" @click="detailMode = 'fields'">字段详情</button><button :aria-pressed="detailMode === 'raw'" @click="detailMode = 'raw'">本行原文</button></div>
            <pre class="event-fields source-view" tabindex="0" :aria-label="detailMode === 'raw' ? '事件本行原文' : '事件字段详情'">{{ detailMode === 'raw' ? selected.raw : JSON.stringify(selected.fields, null, 2) }}</pre>
            <p class="footnote">{{ detailMode === 'raw' ? '保留这一行的原始 JSON 文本。' : selected.event === 'topology_published' ? 'token 等大整数以文本保留精度；原始写法可切换到“本行原文”。' : '字段来自当前事件；原始 JSON 可切换查看。' }}</p>
          </section>
        </div>
      </template>
    </template>
  </section>
</template>
