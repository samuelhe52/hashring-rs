<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import CaseRing from './CaseRing.vue'
import CaseWindowMetrics from './CaseWindowMetrics.vue'
import ReplayEventList from './ReplayEventList.vue'
import RunFacts from './RunFacts.vue'
import RunReadings from './RunReadings.vue'
import { number, object, request } from '../data'
import { casePlayback, headline, latestPublishedEpoch, scenes, seconds, stageDetails, type Scene } from '../caseReplay'
import { eventName, eventSummary, relativeEventTime } from '../events'
import { replayBounds, visibleCount } from '../history.ts'
import { decodeTopology, type RingModel } from '../topology.ts'
import type { EventLog, RunDetail, RunEvent } from '../types'
import { useReplayClock } from '../useReplayClock'

const props = defineProps<{ run: RunDetail; scene: Scene; modalOpen: boolean }>()
const emit = defineEmits<{ inspect: [event: RunEvent]; topology: [line?: number] }>()
const log = ref<EventLog>({ events: [], issues: [], missing: false })
const loading = ref(true)
const eventError = ref('')
const timeline = ref<HTMLElement | null>(null)
let controller: AbortController | undefined
const visual = ref<'ring' | 'metrics' | 'mechanism'>('ring')
const evidence = ref<'stage' | 'feed'>('stage')
const mechanismOverride = ref<'before' | 'after' | null>(null)

const replay = computed(() => casePlayback(props.scene, log.value.events))
const stages = computed(() => replay.value.stages.map(stage => ({ ...stage, ...stageDetails(stage, replay.value, props.run.summary) })))
const stageEvents = computed(() => stages.value.map(stage => stage.event))
const bounds = computed(() => replayBounds(replay.value.records, props.run.manifest?.started_unix_ms))
const initialEvent = computed(() => stages.value[0]?.event ?? replay.value.records[0])
const { cursor, boundaryLine, playing, speed, pause, seek, toggle } = useReplayClock(
  () => bounds.value,
  () => ({ time: initialEvent.value?.unix_ms ?? bounds.value.start, line: initialEvent.value?.line ?? Infinity }),
)
const count = computed(() => visibleCount(replay.value.records, cursor.value, boundaryLine.value ?? Infinity))
const seen = computed(() => replay.value.records.slice(0, count.value))
const currentEvent = computed(() => seen.value.at(-1))
const stageIndex = computed(() => visibleCount(stageEvents.value, cursor.value, boundaryLine.value ?? Infinity) - 1)
const activeStage = computed(() => stages.value[stageIndex.value])
const reachedStages = computed(() => stages.value.slice(0, stageIndex.value + 1))
const sourceOrigin = computed(() => replay.value.anchor?.unix_ms ?? bounds.value.origin)
const readings = computed(() => headline(props.scene, replay.value, props.run.summary))
const duration = computed(() => bounds.value.end - bounds.value.start)
const config = computed(() => object(props.run.manifest?.config))
const initialNodes = computed(() => {
  const members = replay.value.initialSnapshot?.fields.members
  const peak = number(config.value.node_count)
  if (!Array.isArray(members) || peak === null || peak <= members.length) return undefined
  return props.scene === 'fault' ? members.length + ' + ' + (peak - members.length) + ' 替换' : members.length + '→' + peak
})
const topologyRecords = computed(() => log.value.events.filter(event => event.event === 'topology_published'))
const snapshots = computed(() => replay.value.records.flatMap(event => {
  if (event.event !== 'topology_published') return []
  const model = decodeTopology(event.fields).model
  return model ? [{ event, model }] : []
}))
const snapshotEvents = computed(() => snapshots.value.map(({ event }) => event))
const snapshotIndex = computed(() => visibleCount(snapshotEvents.value, cursor.value, boundaryLine.value ?? Infinity) - 1)
const snapshot = computed(() => snapshots.value[snapshotIndex.value])
const earlierRing = computed<RingModel | null>(() => {
  const initial = snapshots.value.slice(0, Math.max(0, snapshotIndex.value)).find(({ event }) => event === replay.value.initialSnapshot)
  if (!snapshot.value || !initial || initial.model.epoch === snapshot.value.model.epoch) return null
  return initial.model
})
const publishedEpoch = computed(() => latestPublishedEpoch(seen.value))
const missingTargetRing = computed(() => props.scene === 'expansion' &&
  reachedStages.value.some(stage => stage.kind === 'migrated' || stage.kind === 'verified') &&
  !reachedStages.value.some(stage => stage.kind === 'snapshot'))
const ringStatus = computed(() => {
  const displayed = snapshot.value?.model.epoch
  if (publishedEpoch.value && publishedEpoch.value !== displayed) {
    return '已发布 epoch ' + publishedEpoch.value + (displayed ? ' · 环图为记录中的 epoch ' + displayed : ' · 完整快照未记录')
  }
  if (missingTargetRing.value) return '扩容后的完整快照未记录'
  return snapshot.value ? 'events.jsonl 第 ' + snapshot.value.event.line + ' 行' : '当前游标无完整快照'
})
const victim = computed(() => {
  const anchor = replay.value.anchor
  return props.scene === 'fault' && anchor && seen.value.includes(anchor)
    ? String(anchor.fields.node_id ?? anchor.fields.victim ?? '') : undefined
})
const mechanism = computed(() => mechanismOverride.value ?? (
  reachedStages.value.some(stage => ['publish', 'migrated', 'verified', 'snapshot'].includes(stage.kind)) ? 'after' : 'before'
))
const hasMetrics = computed(() => replay.value.records.some(event => event.event === 'metrics_sampled'))
const marks = computed(() => reachedStages.value.map(stage => stage.event))
function jump(event: RunEvent) { seek(event.unix_ms, event.line) }
function step(direction: number) {
  const stage = stages.value[stageIndex.value + direction]
  if (stage) jump(stage.event)
}

async function loadEvents() {
  controller?.abort()
  controller = new AbortController()
  const signal = controller.signal
  loading.value = true
  eventError.value = ''
  try {
    log.value = await request<EventLog>('/api/runs/' + encodeURIComponent(props.run.id) + '/events', signal)
  } catch (failure) {
    if (!signal.aborted) eventError.value = failure instanceof Error ? failure.message : String(failure)
  } finally {
    if (!signal.aborted) {
      if (initialEvent.value) jump(initialEvent.value)
      else seek(bounds.value.start)
      loading.value = false
    }
  }
}
watch(() => props.modalOpen, open => { if (open) pause() })
watch(activeStage, async () => {
  mechanismOverride.value = null
  await nextTick()
  const rail = timeline.value
  const current = rail?.querySelector<HTMLElement>('[aria-current="step"]')
  if (rail && current && rail.scrollWidth > rail.clientWidth) {
    const offset = current.getBoundingClientRect().left - rail.getBoundingClientRect().left
    if (offset < 0 || offset + current.offsetWidth > rail.clientWidth) rail.scrollLeft += offset
  }
})
onMounted(loadEvents)
onBeforeUnmount(() => controller?.abort())
</script>

<template>
  <div v-if="loading" class="dashboard-empty case-loading" role="status">
    <span class="loading-dot"></span><p>正在读取回放记录</p>
  </div>
  <section v-else class="dashboard-content" aria-label="阶段回放">
    <RunFacts :config="config" :nodes="initialNodes" />
    <RunReadings :items="readings" label="本场最终读数" />
    <div v-if="eventError" class="notice error" role="alert"><p>事件读取失败。{{ eventError }}</p><button class="table-link" @click="loadEvents">重新读取</button></div>
    <div v-else-if="log.issues.length" class="notice warning">{{ log.issues.length }} 行事件无法解析，可在原文中核对。</div>
    <div class="case-layout">
      <section class="dashboard-card case-figure" aria-label="随游标变化的实验图">
        <header class="dashboard-card-heading">
          <h2>{{ visual === 'metrics' ? '变更窗口' : '一致性哈希环' }}</h2>
          <nav v-if="scene === 'expansion' || hasMetrics" class="dashboard-card-tabs" aria-label="图表视图">
            <button :aria-pressed="visual === 'ring'" @click="visual = 'ring'">记录的环</button>
            <button v-if="scene === 'expansion'" :aria-pressed="visual === 'mechanism'" @click="visual = 'mechanism'">机制示意</button>
            <button v-if="hasMetrics" :aria-pressed="visual === 'metrics'" @click="visual = 'metrics'">指标</button>
          </nav>
          <span v-else-if="snapshot" class="case-snapshot-tag">实测快照</span>
        </header>
        <div class="case-figure-body">
          <CaseWindowMetrics v-if="visual === 'metrics'" :seen="seen" :records="replay.records" :marks="marks"
            :start="bounds.start" :end="bounds.end" :origin="sourceOrigin" :cursor="cursor"
            :axis-label="scene === 'fault' ? '自故障起算（s）' : '自扩容开始（s）'" />
          <CaseRing v-else :model="snapshot?.model ?? null" :previous="earlierRing" :victim="victim" :mechanism="visual === 'mechanism' ? mechanism : undefined" />
        </div>
        <footer class="dashboard-card-footer">
          <template v-if="visual === 'mechanism'">
            <span>K 的哈希位置固定</span>
            <div class="dashboard-card-tabs" aria-label="机制示意阶段">
              <button :aria-pressed="mechanism === 'before'" @click="mechanismOverride = 'before'">加入前</button>
              <button :aria-pressed="mechanism === 'after'" @click="mechanismOverride = 'after'">加入后</button>
            </div>
          </template>
          <span v-else-if="visual === 'metrics'">{{ config.mode === 'scaling' ? '窗口 p99 取采样时所在阶段，缺失处断开。' : '竖线标注变更事件，缺失窗口断开。' }}</span>
          <template v-else>
            <span :class="{ 'snapshot-stale': missingTargetRing || (publishedEpoch && publishedEpoch !== snapshot?.model.epoch) }">{{ ringStatus }}</span>
            <button v-if="topologyRecords.length" class="table-link" aria-haspopup="dialog" @click="emit('topology', snapshot?.event.line)">快照详情 ↗</button>
          </template>
        </footer>
      </section>

      <section id="case-evidence" class="dashboard-card case-evidence" aria-label="阶段证据" tabindex="-1">
        <header class="dashboard-card-heading">
          <div class="dashboard-card-tabs evidence-tabs" aria-label="回放证据">
            <button :aria-pressed="evidence === 'stage'" @click="evidence = 'stage'">本阶段</button>
            <button :aria-pressed="evidence === 'feed'" @click="evidence = 'feed'">事件进程</button>
          </div>
          <span v-if="activeStage" class="case-stage-counter">{{ stageIndex + 1 }} / {{ stages.length }} 阶段</span>
        </header>
        <div v-if="evidence === 'stage'" class="case-evidence-body">
          <article v-if="activeStage" :key="activeStage.event.line" class="case-stage-reading" :data-tone="activeStage.tone">
            <div class="case-stage-time"><i aria-hidden="true"></i><time>{{ activeStage.elapsed < 0 ? '−' : '+' }}{{ seconds(Math.abs(activeStage.elapsed)) }} s</time><span>{{ scene === 'fault' ? '自故障起算' : '相对扩容开始' }}</span></div>
            <h2>{{ activeStage.title }}</h2>
            <p>{{ activeStage.note }}</p>
            <dl v-if="activeStage.facts.length" class="case-stage-facts">
              <div v-for="fact in activeStage.facts" :key="fact.label">
                <dt>{{ fact.label }}</dt><dd><button v-if="fact.source" class="table-link" :aria-label="fact.label + ' · 查看事件第 ' + fact.source.line + ' 行'" @click="emit('inspect', fact.source)">{{ fact.value }} ↗</button><template v-else>{{ fact.value }}</template></dd>
              </div>
            </dl>
          </article>
          <article v-else-if="currentEvent" class="case-stage-reading">
            <div class="case-stage-time"><time>{{ relativeEventTime(currentEvent.unix_ms, sourceOrigin) }}</time></div>
            <h2>{{ eventName(currentEvent.event) }}</h2><p>{{ eventSummary(currentEvent) || '此时间点尚未进入案例阶段。' }}</p>
          </article>
          <p v-else class="empty-inline">{{ log.missing ? '本场未找到 events.jsonl。' : log.events.length ? '游标尚未到达首条事件。' : '本场未记录可回放事件。' }}</p>
        </div>
        <div v-else class="case-feed-body">
          <ReplayEventList :events="seen" :total="replay.records.length" :origin="sourceOrigin" :selected="currentEvent?.line" animate compact
            empty="当前游标没有可用事件。" @jump="jump" />
        </div>
        <footer class="dashboard-card-footer">
          <template v-if="evidence === 'stage' && activeStage">
            <span>{{ activeStage.eventName }} · #{{ activeStage.event.line }}</span>
            <button class="table-link" aria-haspopup="dialog" @click="emit('inspect', activeStage.event)">事件原文 ↗</button>
          </template>
          <template v-else-if="currentEvent">
            <span>{{ eventName(currentEvent.event) }} · #{{ currentEvent.line }}</span>
            <button class="table-link" aria-haspopup="dialog" @click="emit('inspect', currentEvent)">事件原文 ↗</button>
          </template>
        </footer>
      </section>

      <section class="case-transport" aria-label="阶段与回放控制">
        <div v-if="stages.length" ref="timeline" class="case-stage-line" role="group" aria-label="阶段时间线">
          <button v-for="(stage, index) in stages" :key="stage.event.line" :aria-current="index === stageIndex ? 'step' : undefined" :data-reached="index <= stageIndex"
            @click="jump(stage.event)">
            <i aria-hidden="true"></i><span><strong>{{ stage.label }}</strong><time>{{ seconds(stage.elapsed) }} s</time></span>
          </button>
        </div>
        <p v-else class="case-no-stages">该次运行未记录{{ scenes[scene].title }}的阶段事件。</p>
        <div v-if="replay.records.length" class="case-transport-controls">
          <div class="case-play-controls">
            <button class="button secondary case-play" :disabled="!duration" @click="toggle">
              <svg viewBox="0 0 16 16" aria-hidden="true"><path v-if="playing" d="M4 3h3v10H4zm5 0h3v10H9z" /><path v-else d="M5 2.5v11L13 8z" /></svg>
              {{ playing ? '暂停' : cursor >= bounds.end && duration ? '重播' : '播放' }}
            </button>
            <select v-model="speed" aria-label="回放速度"><option v-for="value in [1, 2, 5, 10]" :key="value" :value="value">{{ value }}×</option></select>
          </div>
          <div class="case-scrubber">
            <input class="replay-seek" type="range" aria-label="回放进度" :aria-valuetext="relativeEventTime(cursor, sourceOrigin)" :min="bounds.start" :max="bounds.end"
              :value="cursor" :disabled="!duration" :style="{ '--playback-progress': (duration ? (cursor - bounds.start) / duration * 100 : 0) + '%' }"
              step="1" @input="seek(Number(($event.target as HTMLInputElement).value))" />
            <output title="游标对应的事件时间">{{ relativeEventTime(cursor, sourceOrigin) }}</output>
          </div>
          <div class="case-step-controls">
            <button class="button secondary" :disabled="stageIndex <= 0" @click="step(-1)"><span aria-hidden="true">←</span> 上一阶段</button>
            <button class="button primary" :disabled="stageIndex >= stages.length - 1" @click="step(1)">下一阶段 <span aria-hidden="true">→</span></button>
          </div>
        </div>
      </section>
    </div>
  </section>
</template>
