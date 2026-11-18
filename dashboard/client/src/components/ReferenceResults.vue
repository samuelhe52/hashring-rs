<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import DataChart from './DataChart.vue'
import { colors, formatNumber as fmt, number, request, requestText, stages } from '../data'
import { nodeColor, nodePalette } from '../topology.ts'
import type { ReferenceView } from '../navigation'
import type { RunDetail } from '../types'

const props = defineProps<{ view: ReferenceView }>()
const emit = defineEmits<{ view: [view: ReferenceView] }>()
const acceptance = [
  { requirement: '扩容期读成功率 ≥99.9%，写最终一致', observed: '11 次闭环运行，0 应用层错误、0 正确性违规。',
    status: '缺窗口分母', tone: 'pending', detail: '报告给出本组观察结果。未列同一扩容窗口的 GET 成功数与分母，百分比未计算。',
    source: 'scaling-under-load.md', section: '正式结果 §0.1' },
  { requirement: 'N ≥ 10 时扩容换主键 ≤10%', observed: '10→11 · 64,761 / 1,000,000 键（6.48%）',
    status: '达标', tone: 'pass', detail: '10→11 快照搬运 64,926 条记录。9→10、百万键对照为 10.53%，快照搬运 105,460 条记录。两组均为 V=128、seed 1。',
    source: 'acceptance-metrics.md', section: '迁移率' },
  { requirement: '主节点故障后服务恢复 ≤30 s', observed: '首次成功读约 5.45 s',
    status: '达标', tone: 'pass', detail: 'FirstSuccessor，3 活节点 + 1 替换，1,000 键。三次自故障起算的读恢复为 5.449、5.444、5.458 s。',
    source: 'acceptance-metrics.md', section: '故障恢复时间', supplement: 'fault-under-load.md' },
  { requirement: '10 节点、百万键平均 GET <10 ms', observed: 'GET 均值上界 ≤3.2 ms',
    status: '达标', tone: 'pass', detail: '10 节点、RF=3、OwnerOnly、128 B、Zipf 0.99、80:15:5，实际负载 40,000–100,000 ops/s。报告依据 p99 与最大值估算上界。',
    source: 'acceptance-metrics.md', section: '阶段 6 容量基线' },
  { requirement: 'V=200 时物理节点键数差异 ≤5%', observed: '19.3% ·（最大 − 最小）/ 平均',
    status: '未达标', tone: 'fail', detail: '10 节点、100,000 键、均匀分布、RF=3、seed 1。同组 CV 为 6.2%。',
    source: 'acceptance-metrics.md', section: '均衡度', balance: true },
  { requirement: '至少覆盖 3 种扩缩容场景', observed: '5 种：往返、单扩、单缩、替换、连续变更',
    status: '达标', tone: 'pass', detail: '场景分别对应 round-trip、grow、shrink、replace、churn。',
    source: 'scaling-under-load.md', section: '正式矩阵 §0.1' },
  { requirement: '单节点下线迁移率 ≤10%', observed: '10→9 为 10.51% · 11→10 为 6.45%',
    status: '部分达标', tone: 'pending', detail: '100,000 键、seed 1。10→9 有 10,508 键换主，直接合并路径的快照搬运记录数为 0。',
    source: 'acceptance-metrics.md', section: '单节点下线的 10% 线' },
  { requirement: '对比 pull / push 更新策略', observed: '已实现定时拉取',
    status: '缺对照', tone: 'pending', detail: '团队报告记录了 5 s 拉取，推送路径尚未实现。',
    source: 'acceptance-metrics.md', section: '阶段 5 pull vs push' },
]

interface CampaignPoint { rate: number; p50: number; p99: number; distribution: string }

function readCampaign(source: string) {
  const [header, ...rows] = source.trim().split(/\r?\n/)
  const columns = header!.split('\t')
  return rows.flatMap(row => {
    const fields = Object.fromEntries(row.split('\t').map((value, index) => [columns[index], value]))
    if (fields.kind !== 'campaign' || fields.virtual_nodes !== '128' || fields.reps !== '3' ||
      !fields.achieved_rate || !fields.get_p50 || !fields.get_p99) return []
    const point = {
      distribution: fields.distribution!, rate: Number(fields.achieved_rate), p50: Number(fields.get_p50), p99: Number(fields.get_p99),
    }
    return [point.rate, point.p50, point.p99].every(Number.isFinite) ? [point] : []
  })
}

const campaign = ref<CampaignPoint[]>([])
const balance = ref<RunDetail | null>(null)
const distribution = ref('uniform')
const loading = ref(false)
const error = ref('')
let controller: AbortController | undefined
const rows = computed(() => campaign.value.filter(point => point.distribution === distribution.value).sort((a, b) => a.rate - b.rate))
const first = computed(() => rows.value[0])
const last = computed(() => rows.value.at(-1))
const campaignLabel = computed(() => 'GET 延迟—实际速率 · ' + (distribution.value === 'uniform' ? '均匀分布' : 'Zipf 0.99'))
const campaignSource = 'latency-acceptance-20261022-172621 · 10 节点 / 100k 键 / V128 / RF3 / OwnerOnly / 90:9:1 / 各档 3 次中位数'
const chartGrid = { top: 32, bottom: 62, left: 62, right: 28, outerBoundsMode: 'same' }
const splitLine = { lineStyle: { color: '#e5e0d7', type: 'dashed' } }
const loadChart = computed(() => ({
  color: [colors[1], colors[0]],
  grid: chartGrid,
  legend: { bottom: 4 },
  tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => fmt(Array.isArray(value) ? value.at(-1) : value, 2) + ' µs' },
  xAxis: { type: 'value', min: 0, name: '实际成功速率（ops/s）', nameLocation: 'middle', nameGap: 29, axisLabel: { hideOverlap: true } },
  yAxis: { type: 'value', min: 0, name: 'GET 延迟（µs）', splitLine },
  series: (['p50', 'p99'] as const).map((metric, index) => ({
    type: 'line', name: 'GET ' + metric, data: rows.value.map(row => [row.rate, row[metric]]),
    symbol: index === 0 ? 'circle' : 'diamond', symbolSize: 9, lineStyle: { width: 2.5 },
  })),
}))
const nodes = computed(() => Object.entries(stages(balance.value?.summary ?? null)[0]?.[1].load_balance?.per_node ?? {})
  .flatMap(([id, metrics]) => number(metrics.owned_keys) !== null ? [{ id, keys: metrics.owned_keys! }] : [])
  .sort((a, b) => a.id.localeCompare(b.id, 'en', { numeric: true })))
const mean = computed(() => nodes.value.reduce((sum, node) => sum + node.keys, 0) / nodes.value.length)
const spread = computed(() => (Math.max(...nodes.value.map(node => node.keys)) - Math.min(...nodes.value.map(node => node.keys))) / mean.value * 100)
const balanceChart = computed(() => ({
  grid: { ...chartGrid, bottom: 52 },
  tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => fmt(value) + ' 键' },
  xAxis: { type: 'category', data: nodes.value.map(node => node.id), name: '物理节点', nameLocation: 'middle', nameGap: 30,
    axisLabel: { hideOverlap: true, formatter: (id: string) => id.replace(/^node-/, '') } },
  yAxis: { type: 'value', min: 0, name: '主节点负责的键数', splitLine },
  series: [{
    type: 'bar', name: '键数', barMaxWidth: 36,
    data: nodes.value.map(node => ({ value: node.keys, itemStyle: { color: nodeColor(node.id, nodePalette), borderRadius: [4, 4, 0, 0] } })),
    markLine: { symbol: ['none', 'none'], label: { show: false },
      lineStyle: { color: '#514a40', type: 'dashed' }, data: [{ name: '平均', yAxis: mean.value }] },
  }],
}))
const balanceId = encodeURIComponent('@demo/balance-v200')

async function load() {
  controller?.abort()
  if (props.view === 'acceptance' || (props.view === 'load' && campaign.value.length) || (props.view === 'balance' && balance.value)) {
    error.value = ''
    loading.value = false
    return
  }
  controller = new AbortController()
  const signal = controller.signal
  const view = props.view
  loading.value = true
  error.value = ''
  try {
    if (view === 'load') {
      const data = readCampaign(await requestText('/api/evidence/latency-medians.tsv', signal))
      if (!data.length) throw new Error('正式负载表没有匹配的三次重复统计。')
      if (!signal.aborted) campaign.value = data
    } else {
      const data = await request<RunDetail>('/api/runs/' + balanceId, signal)
      if (!signal.aborted) balance.value = data
    }
  } catch (failure) {
    if (!signal.aborted) error.value = failure instanceof Error ? failure.message : String(failure)
  } finally {
    if (!signal.aborted) loading.value = false
  }
}
watch(() => props.view, load, { immediate: true })
onBeforeUnmount(() => controller?.abort())
</script>

<template>
  <section v-if="view === 'acceptance'" class="reference-report">
    <div class="reference-report-intro">
      <p>课程指标采用团队正式记录。本机运行的配置与原文可在运行概览中核对。</p>
      <a class="table-link" href="/api/evidence/acceptance-metrics.md" target="_blank" rel="noopener">完整验收记录 ↗</a>
    </div>
    <div class="table-scroll reference-acceptance-table" tabindex="0" aria-label="课程验收对照">
      <table>
        <thead><tr><th scope="col">课程要求</th><th scope="col">实测与口径</th><th scope="col">判定</th><th scope="col">证据来源</th></tr></thead>
        <tbody><tr v-for="row in acceptance" :key="row.requirement">
          <th scope="row">{{ row.requirement }}</th>
          <td><strong>{{ row.observed }}</strong><p>{{ row.detail }}</p>
            <button v-if="row.balance" class="table-link" @click="emit('view', 'balance')">查看各节点实际键数 →</button>
          </td>
          <td><span class="reference-verdict" :data-tone="row.tone">{{ row.status }}</span></td>
          <td><a class="table-link" :href="'/api/evidence/' + row.source" target="_blank" rel="noopener">{{ row.section }} ↗</a>
            <p v-if="row.supplement"><a class="table-link" :href="'/api/evidence/' + row.supplement" target="_blank" rel="noopener">带负载故障报告 ↗</a></p>
          </td>
        </tr></tbody>
      </table>
    </div>
  </section>
  <section v-else class="reference-report">
    <nav class="view-switch" aria-label="负载结果">
      <button :aria-pressed="view === 'load'" @click="emit('view', 'load')">负载与延迟</button>
      <button :aria-pressed="view === 'balance'" @click="emit('view', 'balance')">物理节点键数</button>
    </nav>
    <p v-if="loading" class="empty-inline" role="status">正在读取实验数据……</p>
    <div v-else-if="error" class="notice error" role="alert"><p>{{ error }}</p><button class="table-link" @click="load">重试</button></div>
    <template v-else-if="view === 'load'">
      <div class="reference-report-chart-heading">
        <div><h3>{{ campaignLabel }}</h3><p>10 节点 · 100,000 键 · V 128 · RF 3 · OwnerOnly · 每档 3 次</p></div>
        <select v-model="distribution" aria-label="键访问分布"><option value="uniform">均匀分布</option><option value="zipf:0.99">Zipf 0.99</option></select>
      </div>
      <div v-if="first && last" class="reference-report-values">
        <div><span>实际成功速率</span><strong>{{ fmt(first.rate) }} → {{ fmt(last.rate) }} <small>ops/s</small></strong></div>
        <div><span>GET p50</span><strong>{{ fmt(first.p50) }} → {{ fmt(last.p50) }} <small>µs</small></strong></div>
        <div><span>GET p99</span><strong>{{ fmt(first.p99) }} → {{ fmt(last.p99) }} <small>µs</small></strong></div>
      </div>
      <DataChart v-if="rows.length" :option="loadChart" :label="campaignLabel + '（µs）'" :source="campaignSource" animate-updates exportable />
      <p v-else class="empty-inline">正式表中没有该分布的可用记录。</p>
      <p class="reference-report-source">
        每点采用三次运行的中位数。每档预热 10 s、计时 60 s。
        <a class="table-link" href="/api/evidence/latency-acceptance.md" target="_blank" rel="noopener">实验报告 ↗</a>
        <a class="table-link" href="/api/evidence/latency-medians.tsv" target="_blank" rel="noopener">中位数 TSV ↗</a>
      </p>
    </template>
    <template v-else>
      <div class="reference-report-chart-heading"><div><h3>虚拟节点均衡复测</h3><p>本机复测 · 10 节点 · 100,000 键 · V 200 · RF 3 · seed 1 · 均匀分布</p></div></div>
      <div v-if="nodes.length" class="reference-report-values">
        <div><span>平均键数</span><strong>{{ fmt(mean) }} <small>键</small></strong></div>
        <div><span>（最大 − 最小）/ 平均</span><strong>{{ fmt(spread, 2) }}<small>%</small></strong></div>
        <div><span>课程门槛</span><strong>≤5<small>%</small></strong></div>
      </div>
      <DataChart v-if="nodes.length" :option="balanceChart" label="V=200 · 各物理节点拥有的键数" source="balance-v200 · 10 节点 / 100k 键 / RF3 / seed 1 / 本机复测" exportable />
      <p v-else class="empty-inline">复测结果未记录各物理节点拥有的键数。</p>
      <p class="reference-report-source">
        虚线标出平均键数。验收判定引用团队报告的 19.3%。
        <a class="table-link" :href="'/api/runs/' + balanceId + '/raw?file=summary.json'" target="_blank" rel="noopener">复测原始结果 ↗</a>
        <a class="table-link" href="/api/evidence/acceptance-metrics.md" target="_blank" rel="noopener">团队验收记录 ↗</a>
      </p>
    </template>
  </section>
</template>
