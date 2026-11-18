<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import RunReplay from './RunReplay.vue'
import type { EventLog, ExperimentFeed, ExperimentSession, RunDetail } from '../types'
import { formatNumber as fmt, formatTime, request, text } from '../data'
import { downloadBlob } from '../download'

const emit = defineEmits<{ result: [id: string] }>()
const modes = [
  ['latency', '延迟负载'], ['correctness', '正确性与迁移'], ['performance', '性能'],
  ['availability', '可用性'], ['scaling', '扩缩容'], ['fault', '故障负载'],
] as const
type Mode = (typeof modes)[number][0]

const baseSettings = { nodes: 8, keys: 100_000, rf: 3, concurrency: 64, valueBytes: 128, seed: '1', virtualNodes: 128,
  policy: 'owner-only', workerThreads: 2 as number | '' }
const capacityLoad = { rates: '40000,60000,80000,90000,100000', warmup: 3, duration: 10,
  mix: '80:15:5', distribution: 'zipf:0.99', maxInFlight: 4096, dedupMiB: 256 }
const latencyPresets = {
  capacity: capacityLoad,
  query: { ...capacityLoad, rates: '2000,5000,10000', mix: '100:0:0' },
  balance: { ...capacityLoad, virtualNodes: 200, rates: '20000', mix: '90:9:1',
    distribution: 'uniform', maxInFlight: 1024, dedupMiB: 128 },
}
const scalingScenarios = [
  { value: 'round-trip', label: '扩容再缩容' },
  { value: 'replace', label: '节点替换' },
  { value: 'churn', label: '连续扩缩容' },
] as const
const modePresets = {
  correctness: {},
  performance: {},
  availability: { availabilityRounds: 3, virtualNodes: 1024, policy: 'first-successor' },
  scaling: { scalingScenario: 'round-trip' },
  fault: { keys: 1_000, concurrency: 16, policy: 'first-successor' },
}
const phases = { starting: '正在启动', running: '实验运行中', stopping: '正在停止', succeeded: '实验成功', failed: '实验失败', stopped: '已停止' }
const emptyLog = (): EventLog => ({ events: [], issues: [], missing: false })
const feed = ref<ExperimentFeed>({ session: null, log: emptyLog(), stderr: '', streamError: '' })
const session = computed(() => feed.value.session)
const active = computed(() => session.value !== null && session.value.endedAt === null)
const completedRun = ref<RunDetail>()
const completedId = computed(() => !active.value && session.value?.hasResult ? session.value.id : '')
const editing = ref(false)
const connected = ref(false)
const connectionError = ref('')
const actionError = ref('')
const action = ref('')
const now = ref(Date.now())
const kind = ref<'form' | 'yaml'>('form')
const yaml = ref('')
const form = reactive({
  mode: 'latency' as Mode, ...baseSettings, ...latencyPresets.capacity,
  availabilityRounds: 3, scalingScenario: 'round-trip',
})
const name = ref(freshName())
const prepared = ref<{ id: string; command: string; script: string } | null>(null)
const autoOpen = ref(true)
let launchedId: string | null = null
const input = computed(() => kind.value === 'yaml' ? { kind: kind.value, name: name.value, yaml: yaml.value } : { kind: kind.value, name: name.value, ...form })
const copied = ref(false)
const pollTopology = ref(false)
const observed = ref<{
  observedAt: number
  ring: { epoch: number | string }
  replicas: {
    topology_epoch: number | string
    range_count: number
    under_replicated_count: number
    nodes: { node_id: string; leased: boolean; suspected: boolean; fenced: boolean }[]
  }
} | null>(null)
const topologyError = ref('')
const elapsed = computed(() => session.value ? Math.max(0, ((session.value.endedAt ?? now.value) - session.value.startedAt) / 1000) : 0)
let source: EventSource | undefined
let clock: ReturnType<typeof setInterval> | undefined
let topologyTimer: ReturnType<typeof setInterval> | undefined
let topologyRequest: AbortController | undefined

function freshName() {
  return 'dashboard-' + new Date().toISOString().replace(/[T:.Z-]/g, '')
}

function applyLatencyPreset(preset: keyof typeof latencyPresets) {
  Object.assign(form, baseSettings, latencyPresets[preset])
}

function applyPreset(mode: Mode) {
  if (mode === 'latency') applyLatencyPreset('capacity')
  else Object.assign(form, baseSettings, modePresets[mode])
}

async function upload(event: Event) {
  const file = (event.target as HTMLInputElement).files?.[0]
  if (!file) return
  actionError.value = ''
  try {
    if (file.size > 200 * 1024) throw new Error('YAML 文件不能超过 200 KiB')
    yaml.value = await file.text()
  } catch (error) {
    actionError.value = error instanceof Error ? error.message : String(error)
  }
}

async function post<T>(url: string, body: unknown): Promise<T> {
  const response = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
  const data = await response.json()
  if (!response.ok) throw new Error(data.error || `请求失败（HTTP ${response.status}）`)
  return data as T
}

function connect() {
  source?.close()
  source = new EventSource('/api/experiments/current/stream')
  source.addEventListener('snapshot', event => {
    feed.value = JSON.parse(event.data) as ExperimentFeed
    connected.value = true
    connectionError.value = ''
    if (active.value) editing.value = false
  })
  source.addEventListener('update', event => {
    const data = JSON.parse(event.data) as Pick<ExperimentFeed, 'session' | 'stderr' | 'streamError'> & Pick<EventLog, 'events' | 'issues'>
    const sameRun = data.session?.id === session.value?.id
    const previous = sameRun ? feed.value.log : emptyLog()
    // 按日志行号去重，保留同毫秒的独立记录。
    const byLine = new Map(previous.events.map(item => [item.line, item]))
    for (const item of data.events) byLine.set(item.line, item)
    feed.value = {
      session: data.session,
      log: {
        events: [...byLine.values()].sort((a, b) => a.unix_ms - b.unix_ms || a.line - b.line),
        issues: [...previous.issues, ...data.issues],
        missing: false,
      },
      stderr: data.stderr,
      streamError: data.streamError,
    }
    if (active.value) editing.value = false
  })
  source.onerror = () => { connected.value = false; connectionError.value = '实时连接中断，正在重新连接。实验仍由本地服务管理。' }
}

async function preview() {
  action.value = 'preview'
  actionError.value = ''
  const payload = input.value
  try {
    const result = await post<NonNullable<typeof prepared.value>>('/api/experiments/preview', payload)
    if (input.value === payload) prepared.value = result
  } catch (error) {
    actionError.value = error instanceof Error ? error.message : String(error)
  } finally { action.value = '' }
}

async function start() {
  if (!prepared.value || active.value || action.value) return
  action.value = 'start'
  actionError.value = ''
  launchedId = prepared.value.id
  try {
    const result = await post<ExperimentSession>('/api/experiments', input.value)
    if (session.value?.id !== result.id) feed.value = { session: result, log: emptyLog(), stderr: '', streamError: '' }
    editing.value = false
    prepared.value = null
  } catch (error) {
    launchedId = null
    actionError.value = error instanceof Error ? error.message : String(error)
  } finally { action.value = '' }
}

async function stop() {
  if (!session.value || action.value) return
  action.value = 'stop'
  actionError.value = ''
  try { await post('/api/experiments/current/stop', { id: session.value.id }) }
  catch (error) { actionError.value = error instanceof Error ? error.message : String(error) }
  finally { action.value = '' }
}

async function copy() {
  if (!prepared.value) return
  try { await navigator.clipboard.writeText(prepared.value.command); copied.value = true }
  catch { actionError.value = '剪贴板不可用，可选中命令复制。' }
}

function saveScript() {
  if (!prepared.value) return
  downloadBlob(new Blob([prepared.value.script], { type: 'text/x-shellscript;charset=utf-8' }), prepared.value.id + '.sh')
}

async function readTopology() {
  if (!session.value || !active.value || topologyRequest) return
  const controller = new AbortController()
  topologyRequest = controller
  try {
    observed.value = await request<NonNullable<typeof observed.value>>('/api/experiments/current/topology?id=' + encodeURIComponent(session.value.id), controller.signal)
    topologyError.value = ''
  } catch (error) {
    if (!controller.signal.aborted) topologyError.value = error instanceof Error ? error.message : String(error)
  } finally {
    if (topologyRequest === controller) topologyRequest = undefined
  }
}

function endTopologyPolling() {
  clearInterval(topologyTimer)
  topologyRequest?.abort()
  topologyRequest = undefined
}

function editNew() {
  editing.value = true
  name.value = freshName()
  actionError.value = ''
}

watch(input, () => { prepared.value = null; copied.value = false })
watch(session, run => {
  if (run?.phase !== 'succeeded' || run.id !== launchedId) return
  launchedId = null
  if (autoOpen.value && run.hasResult) emit('result', run.id)
})
watch(completedId, async (id, _, onCleanup) => {
  completedRun.value = undefined
  if (!id) return
  const controller = new AbortController()
  onCleanup(() => controller.abort())
  try {
    const result = await request<RunDetail>('/api/runs/' + encodeURIComponent(id), controller.signal)
    if (!controller.signal.aborted) completedRun.value = result
  } catch { /* The event stream can still be replayed if the summary is unavailable. */ }
})
watch([pollTopology, active, () => session.value?.id], ([enabled, running]) => {
  endTopologyPolling()
  observed.value = null
  topologyError.value = ''
  if (enabled && running) {
    void readTopology()
    topologyTimer = setInterval(readTopology, 4000)
  }
})
onMounted(() => { connect(); clock = setInterval(() => { now.value = Date.now() }, 1000) })
onBeforeUnmount(() => { source?.close(); clearInterval(clock); endTopologyPolling() })
</script>

<template>
  <section class="experiment-view">
    <p v-if="connectionError" class="notice warning" role="status">{{ connectionError }} <button class="table-link" @click="connect">重连</button></p>
    <p v-if="actionError" class="notice error" role="alert">{{ actionError }}</p>
    <template v-if="session && !editing">
      <div class="experiment-banner">
        <div><span class="status-badge" :class="session.phase === 'succeeded' ? 'success' : session.phase === 'failed' ? 'failed' : 'incomplete'"><i></i>{{ phases[session.phase] }}</span><strong>{{ session.id }}</strong><span class="experiment-clock">{{ fmt(elapsed, 1) }} s</span></div>
        <button v-if="active" class="button secondary stop-experiment" :disabled="session.phase === 'stopping' || !!action" @click="stop">停止实验</button>
        <template v-else>
          <button v-if="session.hasResult" class="button primary" @click="emit('result', session.id)">打开结果</button>
          <button class="button secondary" @click="editNew">配置新实验</button>
        </template>
      </div>
      <p v-if="session.message" class="footnote">{{ session.message }}</p>
      <p v-if="feed.streamError" class="notice warning">{{ feed.streamError }}</p>
      <p v-if="active" class="footnote">关掉窗口后实验会继续运行。重新打开可查看或停止。</p>
      <RunReplay :key="session.id" :live="feed" :run="completedRun">
        <template #source>
          <details class="data-disclosure"><summary>运行命令与 stderr</summary><pre class="source-view command-view" tabindex="0">{{ session.command }}</pre><pre class="source-view live-stderr" tabindex="0" aria-label="实验 stderr">{{ feed.stderr || '暂无 stderr 输出' }}</pre></details>
          <div v-if="active" class="replica-panel">
            <label class="checkbox-label"><input v-model="pollTopology" type="checkbox" />轮询实时拓扑与副本状态</label>
            <p class="footnote">每 4 秒查询一次协调器。正式测量时可关闭查询。</p>
            <p v-if="topologyError" class="text-warning">{{ topologyError }}</p>
            <template v-if="observed">
              <p>拓扑 epoch {{ text(observed.ring.epoch) }} · 副本状态 epoch {{ text(observed.replicas.topology_epoch) }}</p>
              <p class="footnote">{{ formatTime(observed.observedAt) }} · {{ observed.replicas.range_count }} 个区间 · {{ observed.replicas.under_replicated_count }} 个副本不足</p>
              <div v-if="observed.replicas.nodes.length" class="table-scroll"><table><thead><tr><th>节点</th><th>租约</th><th>疑似故障</th><th>隔离</th></tr></thead><tbody><tr v-for="node in observed.replicas.nodes" :key="node.node_id"><th scope="row">{{ node.node_id }}</th><td>{{ text(node.leased) }}</td><td>{{ text(node.suspected) }}</td><td>{{ text(node.fenced) }}</td></tr></tbody></table></div>
            </template>
          </div>
        </template>
      </RunReplay>
    </template>
    <p v-else-if="!connected && !connectionError" class="empty-inline" role="status">正在连接本地实验服务……</p>
    <form v-else class="experiment-form" @submit.prevent="preview">
      <div class="experiment-fields">
        <div class="segmented" aria-label="实验配置方式"><button type="button" :aria-pressed="kind === 'form'" @click="kind = 'form'">表单配置</button><button type="button" :aria-pressed="kind === 'yaml'" @click="kind = 'yaml'">YAML 配置</button></div>
        <label class="form-field">结果目录名<input v-model.trim="name" required maxlength="80" pattern="[A-Za-z0-9][A-Za-z0-9_\-]{0,79}" /></label>
        <template v-if="kind === 'form'">
          <div v-if="form.mode === 'latency'" class="command-actions" role="group" aria-label="延迟负载预设">
            <button type="button" class="button secondary" @click="applyLatencyPreset('capacity')">容量阶梯</button>
            <button type="button" class="button secondary" @click="applyLatencyPreset('query')">查询延迟</button>
            <button type="button" class="button secondary" @click="applyLatencyPreset('balance')">虚拟节点均衡</button>
          </div>
          <div class="form-grid">
            <label class="form-field">模式<select v-model="form.mode" @change="applyPreset(form.mode)"><option v-for="[mode, label] in modes" :key="mode" :value="mode">{{ label }}</option></select></label>
            <label class="form-field">{{ form.mode === 'availability' || form.mode === 'fault' ? '节点总数（含替换）' : '峰值节点数' }}<input v-model.number="form.nodes" type="number" min="2" required /></label>
            <label class="form-field">集群总键数<input v-model.number="form.keys" type="number" min="1" required /></label>
            <label class="form-field">并发度<input v-model.number="form.concurrency" type="number" min="1" required /></label>
            <label class="form-field">工作线程数
              <input v-model.number="form.workerThreads" type="number" min="1" step="1" placeholder="留空自动"
                title="压测客户端和每个数据节点各使用此数量。" aria-description="压测客户端和每个数据节点各使用此数量。留空时自动分配。" />
            </label>
            <label class="form-field">值大小（B）<input v-model.number="form.valueBytes" type="number" min="1" required /></label>
            <label class="form-field">副本数 RF<input v-model.number="form.rf" type="number" min="1" required /></label>
            <label class="form-field">ACK 策略<select v-model="form.policy"><option value="owner-only">OwnerOnly</option><option value="first-successor">FirstSuccessor</option><option value="all-replicas">AllReplicas</option></select></label>
            <label class="form-field">哈希种子<input v-model.trim="form.seed" inputmode="numeric" pattern="[0-9]{1,20}" required /></label>
            <label class="form-field">虚拟位置 / 节点<input v-model.number="form.virtualNodes" type="number" min="1" required /></label>
            <label v-if="form.mode === 'availability'" class="form-field">连续故障轮数<input v-model.number="form.availabilityRounds" type="number" min="1" required /></label>
            <label v-if="form.mode === 'scaling'" class="form-field">扩缩容场景
              <select v-model="form.scalingScenario" aria-label="扩缩容场景" @change="form.nodes = baseSettings.nodes">
                <option v-for="scenario in scalingScenarios" :key="scenario.value" :value="scenario.value">{{ scenario.label }}</option>
              </select>
            </label>
          </div>
          <fieldset v-if="form.mode === 'latency'" class="latency-fields">
            <legend>负载阶段</legend>
            <label class="form-field">目标速率（ops/s，逗号分隔）<input v-model.trim="form.rates" required /></label>
            <div class="form-grid">
              <label class="form-field">预热（秒）<input v-model.number="form.warmup" type="number" min="0" required /></label>
              <label class="form-field">正式（秒）<input v-model.number="form.duration" type="number" min="1" required /></label>
              <label class="form-field">GET:PUT:DELETE<input v-model.trim="form.mix" required /></label>
              <label class="form-field">键分布<input v-model.trim="form.distribution" required /></label>
              <label class="form-field">在途请求上限<input v-model.number="form.maxInFlight" type="number" min="1" required /></label>
              <label class="form-field">每节点回执预算（MiB）<input v-model.number="form.dedupMiB" type="number" min="1" required /></label>
            </div>
          </fieldset>
        </template>
        <template v-else>
          <label class="form-field">上传 YAML<input type="file" accept=".yaml,.yml,text/yaml" @change="upload" /></label>
          <label class="form-field">YAML 内容<textarea v-model="yaml" rows="11" spellcheck="false" required placeholder="mode: latency"></textarea></label>
          <p class="footnote">启动时使用上面的结果目录名。YAML 中的其他字段由实验程序校验。</p>
        </template>
      </div>
      <aside class="command-panel">
        <h3>启动命令</h3>
        <p class="footnote">每次运行使用新的结果目录。先核对命令，再启动。</p>
        <button type="submit" class="button secondary" :disabled="!!action || !connected">{{ action === 'preview' ? '生成中' : '预览命令' }}</button>
        <template v-if="prepared">
          <pre class="source-view command-view" tabindex="0">{{ prepared.command }}</pre>
          <p v-if="kind === 'yaml'" class="footnote">网页启动时写入 YAML。终端复跑可下载包含 YAML 的脚本。</p>
          <label class="checkbox-label"><input v-model="autoOpen" type="checkbox" />成功后打开结果</label>
          <div class="command-actions">
            <button type="button" class="button secondary" @click="copy">{{ copied ? '已复制' : '复制命令' }}</button>
            <button type="button" class="button secondary" @click="saveScript">下载 .sh</button>
            <button type="button" class="button primary" :disabled="!!action || !connected" @click="start">{{ action === 'start' ? '正在启动' : '启动实验' }}</button>
          </div>
        </template>
      </aside>
    </form>
  </section>
</template>
