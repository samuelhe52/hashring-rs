<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { formatNumber as fmt } from '../data'
import {
  assignmentAt, decodeTopology, HASH_SPACE, nodeColor, nodePalette, ringAngle, ringArc, ringBand, ringPoint, spaceShare, topologySegments,
  type RingModel,
} from '../topology.ts'

const props = defineProps<{ model: RingModel | null; previous?: RingModel | null; victim?: string; mechanism?: 'before' | 'after' | 'removed'; derived?: boolean }>()
const focusedNode = ref('')
const hovered = ref<bigint>()
const selected = ref<bigint>()
const illustrativeNodes = ['node-1', 'node-2', 'node-3', 'node-4']
const nodeLabel = (id: string) => props.mechanism ? (['A', 'B', 'C', 'D'][illustrativeNodes.indexOf(id)] ?? id) : id
const color = (id: string) => nodeColor(id, nodePalette)
const exampleToken = HASH_SPACE * 32n / 100n

function illustrativeRing(after: boolean) {
  const members = illustrativeNodes.slice(0, after ? 4 : 3)
  const positions = [14n, 57n, 84n, 44n]
  return decodeTopology({
    members: members.map(node_id => ({ node_id })),
    desired_replication_factor: 3,
    epoch: after ? 2 : 1,
    assignments: members.map((node_id, index) => ({ node_id, token: (HASH_SPACE * positions[index]! / 100n).toString() })),
  }).model
}
const exampleBefore = illustrativeRing(false)
const exampleAfter = illustrativeRing(true)
const model = computed(() => props.mechanism ? (props.mechanism === 'after' ? exampleAfter : exampleBefore) : props.model)
const previous = computed(() => {
  if (props.mechanism === 'after') return exampleBefore
  if (props.mechanism === 'removed') return exampleAfter
  return props.mechanism ? null : props.previous ?? null
})
const ranges = computed(() => model.value?.ranges.map(range => ({ ...range, path: ringBand(range.start, range.length, 122, 150) })) ?? [])
const changed = computed(() => model.value && previous.value ? topologySegments(model.value, previous.value)
  .filter(part => part.changed)
  .map(part => ({ ...part, path: ringBand(part.start, part.length, 122, 150), arc: ringArc(part.start, part.length, 136), edge: ringBand(part.start, part.length, 152, 157) })) : [])
const changedShare = computed(() => spaceShare(changed.value.reduce((sum, part) => sum + part.length, 0n)))
const atRisk = computed(() => !props.mechanism && model.value?.members.some(member => member.id === props.victim) ? props.victim : undefined)
const outgoing = computed(() => previous.value?.members.filter(member => !model.value?.members.some(current => current.id === member.id)) ?? [])
const incoming = computed(() => model.value?.members.filter(member => previous.value && !previous.value.members.some(old => old.id === member.id)) ?? [])
const vacancy = computed(() => ranges.value.filter(range => range.owner === atRisk.value))
const takeoverShares = computed(() => {
  const shares = new Map<string, number>()
  if (props.mechanism || atRisk.value || !outgoing.value.length) return shares
  const departed = new Set(outgoing.value.map(member => member.id))
  const lengths = new Map<string, bigint>()
  for (const part of changed.value) {
    if (departed.has(part.previousOwner)) lengths.set(part.owner, (lengths.get(part.owner) ?? 0n) + part.length)
  }
  for (const [id, length] of lengths) shares.set(id, spaceShare(length))
  return shares
})
const legendGroups = computed(() => {
  const members = model.value?.members.filter(member => member.id !== atRisk.value) ?? []
  const shares = takeoverShares.value
  if (!shares.size) return [{ title: props.mechanism ? '节点归属示意' : '物理节点', members, takeover: false }]
  return [
    { title: '接管方', members: members.filter(member => shares.has(member.id)), takeover: true },
    { title: '其他节点', members: members.filter(member => !shares.has(member.id)), takeover: false },
  ].filter(group => group.members.length)
})
const arrivals = computed(() => incoming.value.flatMap(member => {
  const position = model.value?.assignments.find(assignment => assignment.node === member.id)
  return position ? [{ id: member.id, point: ringPoint(ringAngle(position.token), 155) }] : []
}))
const animationKey = computed(() => (props.mechanism ?? '') + ':' + (previous.value?.epoch ?? '') + '→' + (model.value?.epoch ?? ''))
const exampleRange = computed(() => model.value && props.mechanism ? assignmentAt(model.value, exampleToken).token : undefined)
const active = computed(() => model.value?.ranges.find(range => range.end === (hovered.value ?? selected.value ?? exampleRange.value)))
const tokenDot = ringPoint(ringAngle(exampleToken), 124)
const tickMarks = Array.from({ length: 36 }, (_, index) => {
  const radius = index % 9 === 0 ? 157 : 161
  return { from: ringPoint(index * 10, radius), to: ringPoint(index * 10, 165) }
})
function selectNode(id: string) {
  focusedNode.value = focusedNode.value === id ? '' : id
  selected.value = focusedNode.value ? model.value?.ranges.find(range => range.owner === id)?.end : undefined
}
watch(model, () => { hovered.value = undefined; selected.value = undefined; focusedNode.value = '' })
</script>

<template>
  <figure v-if="model" class="case-ring" :class="{ 'mechanism-ring': mechanism }">
    <svg class="case-ring-plot" viewBox="0 0 360 360" role="img" :aria-label="mechanism ? '机制示意 · 样例键 K 的主节点归属' : (derived ? '按顺时针推导的接管 · ' : '') + 'epoch ' + model.epoch + ' 哈希环'" @pointerleave="hovered = undefined">
      <circle cx="180" cy="180" r="165" class="case-ring-guide" />
      <circle cx="180" cy="180" r="136" class="case-ring-track" />
      <line v-for="(tick, index) in tickMarks" :key="index" :x1="tick.from[0]" :y1="tick.from[1]" :x2="tick.to[0]" :y2="tick.to[1]" class="case-ring-tick" />
      <text x="180" y="8" text-anchor="middle" class="case-ring-zero">0 / 2⁶⁴</text>
      <g>
        <path v-for="range in ranges" :key="range.end.toString()" :d="range.path" :fill="color(range.owner)"
          class="case-ring-segment" :class="{ 'ring-segment-dim': focusedNode && focusedNode !== range.owner, 'ring-segment-active': active?.end === range.end, 'ring-segment-vacated': range.owner === atRisk }"
          @pointerenter="hovered = range.end" @click="selected = range.end">
          <title>{{ nodeLabel(range.owner) }} · ({{ range.start }}, {{ range.end }}] · 副本 {{ range.replicas.map(nodeLabel).join(' → ') }}</title>
        </path>
      </g>
      <g v-if="vacancy.length" class="case-vacancies" aria-hidden="true">
        <path v-for="range in vacancy" :key="range.end.toString()" :d="ringArc(range.start, range.length, 136)" />
      </g>
      <g v-if="changed.length" :key="'changed-' + animationKey" class="case-changed-edge" aria-hidden="true">
        <path v-for="part in changed" :key="part.end.toString()" :d="part.edge" :fill="color(part.owner)" />
      </g>
      <g v-if="changed.length" :key="animationKey" class="case-ring-transition" aria-hidden="true">
        <path v-for="part in changed" :key="'gap-' + part.end.toString()" :d="part.path" class="case-transfer-gap" />
        <template v-for="part in changed" :key="'old-' + part.end.toString()">
          <path v-if="part.previousOwner !== victim" :d="part.path" :fill="color(part.previousOwner)" class="case-transfer-origin"
            :class="{ 'case-transfer-leaving': outgoing.some(member => member.id === part.previousOwner) }" />
        </template>
        <path v-for="part in changed" :key="'sweep-' + part.end.toString()" :d="part.arc"
          :stroke="color(part.owner)" pathLength="1" class="case-transfer-sweep" />
        <circle v-for="marker in arrivals" :key="marker.id" :cx="marker.point[0]" :cy="marker.point[1]" r="5.5"
          :fill="color(marker.id)" class="case-arrival-marker" />
      </g>
      <circle cx="180" cy="180" r="104" class="case-ring-inner" />
      <text x="180" y="154" text-anchor="middle" class="case-ring-caption">{{ mechanism ? '示例键 K' : 'EPOCH' }}</text>
      <text x="180" y="208" text-anchor="middle" class="case-ring-number" :class="{ 'wide-epoch': model.epoch.length > 6 }">
        {{ mechanism ? nodeLabel(assignmentAt(model, exampleToken).node) : model.epoch }}
      </text>
      <text x="180" y="236" text-anchor="middle" class="case-ring-sub">{{ mechanism ? '顺时针定位主节点' : atRisk ? '等待发布接管拓扑' : model.members.length + ' 节点 · ' + model.assignments.length + ' 虚拟位置' }}</text>
      <circle v-if="mechanism" :cx="tokenDot[0]" :cy="tokenDot[1]" r="6" class="case-key-dot" />
    </svg>
    <figcaption class="case-ring-legend">
      <div class="case-ring-nodes" aria-label="选择节点区间">
        <div v-for="group in legendGroups" :key="group.title" class="case-ring-node-group">
          <strong>{{ group.title }}</strong>
          <p v-if="group.takeover" class="case-ring-share-caption">接管区间占整环比例</p>
          <button v-for="member in group.members" :key="member.id" type="button" :aria-pressed="focusedNode === member.id"
            @click="selectNode(member.id)">
            <i :style="{ background: color(member.id) }" aria-hidden="true"></i>{{ nodeLabel(member.id) }}
            <small v-if="group.takeover" class="case-ring-share">{{ fmt(takeoverShares.get(member.id), 2) }}%</small>
            <small v-else-if="incoming.some(node => node.id === member.id)">已加入</small>
          </button>
        </div>
      </div>
      <div class="case-ring-note">
        <template v-if="atRisk">
          <strong>{{ atRisk }} 已停机</strong><span>{{ fmt(vacancy.length) }} 个区间等待接管</span>
        </template>
        <template v-else-if="mechanism">
          <span>样例键 K 的主节点</span>
          <strong><template v-if="previous && assignmentAt(previous, exampleToken).node !== assignmentAt(model, exampleToken).node">{{ nodeLabel(assignmentAt(previous, exampleToken).node) }} → </template>{{ nodeLabel(assignmentAt(model, exampleToken).node) }}</strong>
        </template>
        <template v-else-if="active">
          <span>选中区间主节点</span><strong>{{ active.owner }}</strong>
          <small v-if="active.replicas.length > 1">后继副本 {{ active.replicas.slice(1).join('、') }}</small>
        </template>
        <template v-else-if="takeoverShares.size">
          <span>接管 {{ outgoing.map(member => member.id).join('、') }} 的区间</span>
          <small>外侧细圈标出换主区间</small>
        </template>
        <template v-else-if="changed.length">
          <span>换主区间覆盖的哈希空间</span><strong>{{ fmt(changedShare, 2) }}%</strong>
        </template>
        <template v-else><span>顺时针分配，区间左开右闭。</span></template>
      </div>
    </figcaption>
  </figure>
  <p v-else class="case-no-ring">游标之前没有完整拓扑快照。</p>
</template>
