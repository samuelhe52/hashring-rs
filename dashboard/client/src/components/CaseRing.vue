<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { formatNumber as fmt } from '../data'
import {
  assignmentAt, decodeTopology, HASH_SPACE, nodeColor, nodePalette, ringAngle, ringBand, ringPoint, spaceShare, topologySegments,
  type RingModel,
} from '../topology.ts'

const props = defineProps<{ model: RingModel | null; previous?: RingModel | null; victim?: string; mechanism?: 'before' | 'after' }>()
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
const previous = computed(() => props.mechanism ? (props.mechanism === 'after' ? exampleBefore : null) : props.previous ?? null)
const parts = computed(() => model.value ? topologySegments(model.value, previous.value) : [])
const changed = computed(() => parts.value.filter(part => part.changed))
const changedShare = computed(() => spaceShare(changed.value.reduce((sum, part) => sum + part.length, 0n)))
const atRisk = computed(() => !props.mechanism && !changed.value.length && model.value?.members.some(member => member.id === props.victim) ? props.victim : undefined)
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
    <svg class="case-ring-plot" viewBox="0 0 360 360" role="img" :aria-label="mechanism ? '机制示意 · 样例键 K 的主节点归属' : 'epoch ' + model.epoch + ' 哈希环'" @pointerleave="hovered = undefined">
      <circle cx="180" cy="180" r="165" class="case-ring-guide" />
      <line v-for="(tick, index) in tickMarks" :key="index" :x1="tick.from[0]" :y1="tick.from[1]" :x2="tick.to[0]" :y2="tick.to[1]" class="case-ring-tick" />
      <text x="180" y="8" text-anchor="middle" class="case-ring-zero">0 / 2⁶⁴</text>
      <g>
        <path v-for="part in parts" :key="part.end.toString()" :d="ringBand(part.start, part.length, 122, 150)"
          :fill="color(part.owner)" class="case-ring-segment" :class="{ 'ring-segment-dim': focusedNode && focusedNode !== part.owner, 'ring-segment-active': active?.end === part.rangeEnd }"
          @pointerenter="hovered = part.rangeEnd" @click="selected = part.rangeEnd">
          <title>{{ nodeLabel(part.owner) }} · ({{ part.start }}, {{ part.end }}] · 副本 {{ part.replicas.map(nodeLabel).join(' → ') }}</title>
        </path>
        <path v-for="part in changed" :key="'change-' + part.end.toString()" :d="ringBand(part.start, part.length, 152, 158)" class="case-ring-change" />
        <template v-if="atRisk">
          <path v-for="part in parts.filter(part => part.owner === atRisk)" :key="'fault-' + part.end.toString()"
            :d="ringBand(part.start, part.length, 152, 158)" class="case-ring-change ring-victim" />
        </template>
      </g>
      <circle cx="180" cy="180" r="104" class="case-ring-inner" />
      <text x="180" y="154" text-anchor="middle" class="case-ring-caption">{{ mechanism ? '示例键 K' : 'EPOCH' }}</text>
      <text x="180" y="208" text-anchor="middle" class="case-ring-number" :class="{ 'wide-epoch': model.epoch.length > 6 }">
        {{ mechanism ? nodeLabel(assignmentAt(model, exampleToken).node) : model.epoch }}
      </text>
      <text x="180" y="236" text-anchor="middle" class="case-ring-sub">{{ mechanism ? '顺时针定位主节点' : model.members.length + ' 节点 · ' + model.assignments.length + ' 虚拟位置' }}</text>
      <circle v-if="mechanism" :cx="tokenDot[0]" :cy="tokenDot[1]" r="6" class="case-key-dot" />
    </svg>
    <figcaption class="case-ring-legend">
      <strong>{{ mechanism ? '节点归属示意' : '物理节点' }}</strong>
      <div class="case-ring-nodes" aria-label="选择节点区间">
        <button v-for="member in model.members" :key="member.id" type="button" :aria-pressed="focusedNode === member.id"
          @click="selectNode(member.id)">
          <i :style="{ background: color(member.id) }" aria-hidden="true"></i>{{ nodeLabel(member.id) }}
          <small v-if="member.id === atRisk">已终止</small><small v-else-if="previous && !previous.members.some(node => node.id === member.id)">已加入</small>
        </button>
      </div>
      <div class="case-ring-note">
        <template v-if="mechanism">
          <span>样例键 K 的主节点</span>
          <strong>{{ nodeLabel(assignmentAt(model, exampleToken).node) }}</strong>
        </template>
        <template v-else-if="active">
          <span>选中区间主节点</span><strong>{{ active.owner }}</strong>
          <small v-if="active.replicas.length > 1">后继副本 {{ active.replicas.slice(1).join('、') }}</small>
        </template>
        <template v-else-if="changed.length">
          <span>换主区间覆盖的哈希空间</span><strong>{{ fmt(changedShare, 2) }}%</strong>
        </template>
        <template v-else-if="atRisk"><span>外圈标出已终止节点原来负责的区间。</span></template>
        <template v-else><span>顺时针分配，区间左开右闭。</span></template>
      </div>
    </figcaption>
  </figure>
  <p v-else class="case-no-ring">游标之前没有完整拓扑快照。</p>
</template>
