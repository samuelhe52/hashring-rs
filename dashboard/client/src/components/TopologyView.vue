<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { RunDetail, RunEvent } from '../types'
import { formatNumber as fmt, number, object, stages, text } from '../data'
import { absoluteEventTime } from '../events'
import { decodeTopology, nodeColor, nodePalette, ringAngle, ringBand, ringPoint, spaceShare, spaceStats, topologySegments } from '../topology.ts'

const props = withDefaults(defineProps<{ run?: RunDetail; events: RunEvent[]; follow?: boolean; initialLine?: number }>(), { follow: false })
const selectedLine = ref<number | null>(props.initialLine ?? null)
const referenceLine = ref<number | null>(null)
const selectedEnd = ref('')
const hoveredEnd = ref('')
const focusedNode = ref('')
const showTokens = ref(false)
const view = defineModel<string>('view', { default: 'range' })
const nodeMetric = ref('space')
const selectedStage = ref('')
const snapshots = computed(() => props.events.filter(event => event.event === 'topology_published'))
const decoded = computed(() => snapshots.value.map(event => decodeTopology(event.fields)))
const currentIndex = computed(() => { const found = snapshots.value.findIndex(event => event.line === selectedLine.value); return props.follow || found < 0 ? snapshots.value.length - 1 : found })
const event = computed(() => snapshots.value[currentIndex.value])
const parsed = computed(() => decoded.value[currentIndex.value])
const model = computed(() => parsed.value?.model ?? null)
const references = computed(() => snapshots.value.slice(0, currentIndex.value))
const reference = computed(() => references.value.find(event => event.line === referenceLine.value) ?? references.value.at(-1))
const previous = computed(() => reference.value ? decoded.value[snapshots.value.findIndex(event => event.line === reference.value?.line)] : undefined)
const parts = computed(() => model.value ? topologySegments(model.value, previous.value?.model ?? null) : [])
const activeRange = computed(() => model.value?.ranges.find(range => range.end.toString() === (hoveredEnd.value || selectedEnd.value)) ?? model.value?.ranges[0])
const activeIndex = computed(() => model.value?.ranges.findIndex(range => range.end === activeRange.value?.end) ?? -1)
const stats = computed(() => model.value ? spaceStats(model.value) : [])
const changedSpace = computed(() => parts.value.filter(part => part.changed).reduce((sum, part) => sum + part.length, 0n))
const changedCount = computed(() => parts.value.filter(part => part.changed).length)
const replicaChanges = computed(() => parts.value.filter(part => part.replicasChanged && !part.changed).length)
const epochs = computed(() => new Set(decoded.value.flatMap(parsed => parsed.model ? [parsed.model.epoch] : [])).size)
const sameEpoch = computed(() => model.value && previous.value?.model && model.value.epoch === previous.value.model.epoch)
const loadStages = computed(() => stages(props.run?.summary ?? null))
const loadStage = computed(() => loadStages.value.find(([key]) => key === selectedStage.value) ?? loadStages.value[0])
const unit = computed(() => ({ space: '%', owned_keys: '键', ops_handled: '次', cpu_percent: '% 单核' })[nodeMetric.value])
const barRows = computed(() => {
  if (nodeMetric.value === 'space') return stats.value.map(node => ({ id: node.id, value: node.share, width: node.share, label: shareLabel(node.length), note: `${node.tokens} 个虚拟位置` }))
  const perNode = Object.entries(object(loadStage.value?.[1].load_balance?.per_node))
  const maximum = Math.max(1, ...perNode.map(([, value]) => number(object(value)[nodeMetric.value]) ?? 0))
  return perNode.map(([id, value]) => { const scalar = number(object(value)[nodeMetric.value]); return { id, value: scalar, width: (scalar ?? 0) / maximum * 100, label: fmt(scalar, 2) + ' ' + unit.value, note: loadStage.value?.[0] ?? '' } })
})
function color(node: string) { return nodeColor(node, nodePalette) }
function shareLabel(length: bigint) { const value = spaceShare(length); return length > 0n && value === 0 ? '<0.0001%' : fmt(value, 4) + '%' }
watch(event, () => { selectedEnd.value = ''; hoveredEnd.value = ''; focusedNode.value = '' })
</script>

<template>
  <section class="topology-view">
    <template v-if="event">
      <div class="topology-controls"><label v-if="!follow">当前快照<select :value="event.line" aria-label="选择拓扑快照" @change="selectedLine = Number(($event.target as HTMLSelectElement).value)"><option v-for="item in snapshots" :key="item.line" :value="item.line">epoch {{ text(item.fields.epoch) }} · #{{ item.line }} · {{ absoluteEventTime(item.unix_ms) }}</option></select></label><span v-else class="unit-tag">当前快照 #{{ event.line }}</span><span class="topology-count">{{ snapshots.length }} 份快照 · {{ epochs }} 个 epoch</span><label class="checkbox-label"><input v-model="showTokens" type="checkbox" />显示虚拟位置</label></div>
      <div v-if="!model" class="notice error" role="alert"><p><strong>无法绘制此快照</strong><span>{{ parsed?.error }}。原文保留在下方，没有用部分 token 拼出一个替代环。</span></p></div>
      <template v-else>
        <div class="topology-layout">
          <figure class="ring-figure"><svg viewBox="0 0 360 360" role="img" :aria-label="'epoch ' + model.epoch + ' 哈希环，' + model.members.length + ' 个成员，' + model.assignments.length + ' 个虚拟位置'" @pointerleave="hoveredEnd = ''"><circle cx="180" cy="180" r="162" fill="none" stroke="var(--rule)" stroke-width="1" /><g :key="event.line" class="ring-reveal"><path v-for="(part, index) in parts" :key="part.end.toString()" :d="ringBand(part.start, part.length)" :fill="color(part.owner)" class="ring-sector" :class="{ 'active-range': part.rangeEnd === activeRange?.end }" :style="{ opacity: focusedNode && focusedNode !== part.owner ? .18 : 1 }" @pointerenter="hoveredEnd = part.rangeEnd.toString()" @click="selectedEnd = part.rangeEnd.toString()"><title>{{ part.owner }} · ({{ part.start.toString() }}, {{ part.end.toString() }}] · 副本 {{ part.replicas.join(' → ') }} · #{{ index + 1 }}</title></path><path v-for="part in parts.filter(part => part.changed || part.replicasChanged)" :key="'change-' + part.end.toString()" :d="ringBand(part.start, part.length, 152, 158)" :fill="part.changed ? 'var(--accent)' : 'var(--teal)'" class="ring-change" /><template v-if="showTokens"><circle v-for="(assignment, index) in model.assignments" :key="index" :cx="ringPoint(ringAngle(assignment.token), 162)[0]" :cy="ringPoint(ringAngle(assignment.token), 162)[1]" r="1.6" :fill="color(assignment.node)" /></template></g><text x="180" y="150" text-anchor="middle" class="ring-center-kicker">TOPOLOGY EPOCH</text><text x="180" y="199" text-anchor="middle" class="ring-center-number" :class="{ 'long-epoch': model.epoch.length > 8 }">{{ model.epoch }}</text><text x="180" y="229" text-anchor="middle" class="ring-center-note">{{ model.members.length }} 节点 · {{ model.assignments.length }} 位置</text><text x="180" y="17" text-anchor="middle" class="ring-zero">0 / 2⁶⁴</text></svg><figcaption>顺时针分配 · 区间左开右闭</figcaption><div class="node-legend"><button v-for="member in model.members" :key="member.id" :aria-pressed="focusedNode === member.id" :title="member.endpoint" @click="focusedNode = focusedNode === member.id ? '' : member.id"><i :style="{ background: color(member.id) }"></i>{{ member.id }}</button></div></figure>
          <div class="ring-inspector">
            <nav class="view-switch" aria-label="拓扑内容"><button :aria-pressed="view === 'range'" @click="view = 'range'">区间与副本</button><button :aria-pressed="view === 'nodes'" @click="view = 'nodes'">节点分布</button></nav>
            <template v-if="view === 'range' && activeRange"><label class="record-selector">区间<select :value="activeRange.end.toString()" aria-label="选择哈希区间" @change="selectedEnd = ($event.target as HTMLSelectElement).value; hoveredEnd = ''"><option v-for="(range, index) in model.ranges" :key="range.end.toString()" :value="range.end.toString()">#{{ index + 1 }} · {{ range.owner }} · {{ range.end.toString() }}</option></select></label><dl class="range-facts"><div><dt>区间 #{{ activeIndex + 1 }} · (start, end]</dt><dd class="range-tokens"><code>{{ activeRange.start.toString() }}</code><span>→</span><code>{{ activeRange.end.toString() }}</code><small v-if="activeRange.start >= activeRange.end">跨越 0</small></dd></div><div><dt>Owner</dt><dd><span class="node-chip" :style="{ color: color(activeRange.owner) }">{{ activeRange.owner }}</span></dd></div><div><dt>Followers · 顺时针去重</dt><dd><span v-for="node in activeRange.replicas.slice(1)" :key="node" class="node-chip" :style="{ color: color(node) }">{{ node }}</span><span v-if="activeRange.replicas.length < 2">无</span></dd></div><div><dt>区间覆盖哈希空间</dt><dd>{{ shareLabel(activeRange.length) }}</dd></div><div><dt>期望 RF / 环可推导的放置数</dt><dd>{{ model.rf }} / {{ activeRange.replicas.length }}</dd></div></dl><p class="footnote">副本列表包含 owner，每个物理节点只选一次。它是环上的放置关系，不表示副本已同步、健康或确认了写入。</p></template>
            <template v-else><div class="comparison-controls topology-node-controls"><select v-model="nodeMetric" aria-label="拓扑节点指标"><option value="space">哈希空间比例</option><template v-if="loadStages.length"><option value="owned_keys">阶段 Owner 键数</option><option value="ops_handled">阶段操作数</option><option value="cpu_percent">阶段 CPU</option></template></select><select v-if="nodeMetric !== 'space'" :value="loadStage?.[0]" aria-label="拓扑负载阶段" @change="selectedStage = ($event.target as HTMLSelectElement).value"><option v-for="[key, value] in loadStages" :key="key" :value="key">{{ key }} · {{ fmt(value.target_rate) }} ops/s</option></select></div><div :key="event.line + nodeMetric + loadStage?.[0]" class="space-bars"><div v-for="(node, index) in barRows" :key="node.id" class="space-bar-row"><div><span><i :style="{ background: color(node.id) }"></i>{{ node.id }}</span><strong>{{ node.label }}</strong></div><span class="space-track"><i class="animated-fill" :style="{ width: node.width + '%', background: color(node.id), animationDelay: Math.min(index * 60, 240) + 'ms' }"></i></span><small>{{ node.note }}</small></div><p v-if="!barRows.length" class="empty-inline">此阶段没有节点记录。</p></div><p class="footnote">{{ nodeMetric === 'space' ? '比例由当前快照的精确区间长度推导，表示哈希空间分工，不是实际键数或请求数。' : '这些是 summary 中的阶段成绩，不与快照或回放时刻实时同步；缺指标显示 —。' }}</p></template>
          </div>
        </div>
        <section class="topology-diff"><div class="topology-controls"><label v-if="references.length">对照快照<select :value="reference?.line" aria-label="选择对照快照" @change="referenceLine = Number(($event.target as HTMLSelectElement).value)"><option v-for="item in references" :key="item.line" :value="item.line">epoch {{ text(item.fields.epoch) }} · #{{ item.line }}</option></select></label><span v-else class="footnote">首份快照，没有更早快照可对照。</span></div><template v-if="previous?.model"><p v-if="sameEpoch && !changedCount && !replicaChanges" class="notice neutral">同一 epoch 的快照，未发现 owner 或副本放置变化。</p><p v-else-if="sameEpoch" class="notice warning">同一 epoch 的放置记录不一致，请核对快照原文。</p><div class="diff-readings"><span><i class="diff-key owner"></i>Owner 改变：{{ changedCount }} 段 · {{ shareLabel(changedSpace) }}</span><span><i class="diff-key replica"></i>仅副本列表改变：{{ replicaChanges }} 段</span></div><p class="footnote">按两份快照的联合 token 边界比较。外圈橙色表示 owner 改变，青色表示仅副本放置改变；这不等于实际迁移键数、复制字节数或耗时。</p></template><p v-else-if="previous?.error" class="notice warning">对照快照无法解读：{{ previous.error }}，未标记变化。</p></section>
        <dl class="topology-metadata"><div><dt>哈希算法</dt><dd>{{ text(event.fields.hash_algorithm) }}</dd></div><div><dt>ACK</dt><dd>{{ text(event.fields.write_ack_policy) }}</dd></div><div><dt>记录时间</dt><dd>{{ absoluteEventTime(event.unix_ms) }}</dd></div></dl>
      </template>
      <details class="data-disclosure"><summary>本份拓扑快照原文 · #{{ event.line }}</summary><pre class="source-view" tabindex="0" aria-label="拓扑快照原文">{{ event.raw }}</pre></details>
    </template>
    <p v-else class="empty-inline">{{ follow ? '当前时刻尚未发布拓扑快照。' : '此运行没有 topology_published 快照，无法绘制哈希环。' }}</p>
  </section>
</template>
