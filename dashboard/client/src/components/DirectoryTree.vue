<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, useId, watch } from 'vue'
import type { DirectoryEntry, DirectoryGroup } from '../navigation'

const props = defineProps<{ groups: DirectoryGroup[]; active: string; expanded: string; hasRun: boolean }>()
const emit = defineEmits<{ choose: [entry: DirectoryEntry, keyboard: boolean]; expand: [id: string] }>()
const viewport = ref<HTMLElement | null>(null)
const tree = ref<HTMLElement | null>(null)
const centered = ref('')
const id = useId()
let observer: ResizeObserver | undefined
let frame = 0
let height = 0
let wheelTarget: number | null = null

function disabled(entry: DirectoryEntry) {
  return !props.hasRun && entry.panel !== null && !['runs', 'experiment', 'reference'].includes(entry.panel)
}

function rows() {
  return [...(tree.value?.querySelectorAll<HTMLElement>('[data-wheel-id]') ?? [])]
    .filter(row => !row.closest('[inert]'))
}

function updateCenter() {
  frame = 0
  const area = viewport.value
  if (!area?.clientHeight) return
  const middle = area.getBoundingClientRect().top + area.clientHeight / 2
  let closest: HTMLElement | undefined
  let distance = Infinity
  for (const row of rows()) {
    const box = row.getBoundingClientRect()
    const offset = Math.abs(box.top + box.height / 2 - middle)
    if (offset < distance) { closest = row; distance = offset }
  }
  centered.value = closest?.dataset.wheelId ?? ''
}

function schedule() { if (!frame) frame = requestAnimationFrame(updateCenter) }

function centerRow(row: HTMLElement, smooth = false) {
  const area = viewport.value
  if (!area?.clientHeight) return
  wheelTarget = null
  const box = row.getBoundingClientRect()
  area.scrollTo({
    top: area.scrollTop + box.top + box.height / 2 - area.getBoundingClientRect().top - area.clientHeight / 2,
    behavior: smooth && !matchMedia('(prefers-reduced-motion: reduce)').matches ? 'smooth' : 'instant',
  })
  schedule()
}

function activeRow() {
  return rows().find(row => row.querySelector('[aria-current="page"]'))
}

function centerActive(smooth = false) {
  const row = activeRow() ?? rows()[0]
  if (row) centerRow(row, smooth)
}

function focusActive() {
  const row = activeRow() ?? rows()[0]
  row?.querySelector<HTMLButtonElement>('.index-link')?.focus({ preventScroll: true })
  if (row) centerRow(row)
}

async function expand(entry: DirectoryEntry) {
  emit('expand', props.expanded === entry.id ? '' : entry.id)
  await nextTick()
  const row = rows().find(item => item.dataset.wheelId === entry.id)
  if (row) centerRow(row)
}

function focusRow(event: FocusEvent) {
  const button = event.target
  if (!(button instanceof HTMLButtonElement) || !button.matches(':focus-visible')) return
  const row = button.closest<HTMLElement>('[data-wheel-id]')
  if (row) centerRow(row)
}

function keys(event: KeyboardEvent) {
  if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return
  const directions: Record<string, number> = { ArrowUp: -1, ArrowDown: 1, Home: -Infinity, End: Infinity }
  const direction = directions[event.key]
  if (direction === undefined) return
  const available = rows().filter(row => !row.querySelector<HTMLButtonElement>('.index-link')?.disabled)
  const index = available.findIndex(row => row.dataset.wheelId === centered.value)
  const next = available[Math.max(0, Math.min(available.length - 1, index + direction))]
  if (!next) return
  event.preventDefault()
  event.stopPropagation()
  next.querySelector<HTMLButtonElement>('.index-link')?.focus({ preventScroll: true })
  centerRow(next)
}

function scrollWheel(event: WheelEvent) {
  const area = viewport.value
  if (event.ctrlKey || !area || !event.deltaY) return
  event.preventDefault()
  const unit = event.deltaMode === WheelEvent.DOM_DELTA_LINE ? 42 : event.deltaMode === WheelEvent.DOM_DELTA_PAGE ? area.clientHeight : 1
  const distance = event.deltaY * unit * 0.6
  wheelTarget = Math.max(0, Math.min(area.scrollHeight - area.clientHeight, (wheelTarget ?? area.scrollTop) + distance))
  area.scrollTo({ top: wheelTarget, behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth' })
}

function endScroll() { wheelTarget = null }

// 标题和边缘处的滚轮也交给目录，正文继续使用自己的滚动区域。
function relayWheel(event: WheelEvent) {
  if (!viewport.value?.contains(event.target as Node)) scrollWheel(event)
}

watch(() => props.active, () => centerActive(true), { flush: 'post' })
watch(() => props.expanded, schedule, { flush: 'post' })
onMounted(() => {
  observer = new ResizeObserver(() => {
    const area = viewport.value
    if (area?.clientHeight && area.clientHeight !== height) {
      height = area.clientHeight
      area.style.setProperty('--wheel-gutter', Math.max(0, (height - 42) / 2) + 'px')
      centerActive()
    }
    schedule()
  })
  if (viewport.value) observer.observe(viewport.value)
  if (tree.value) observer.observe(tree.value)
})
onBeforeUnmount(() => { observer?.disconnect(); cancelAnimationFrame(frame) })
defineExpose({ centerActive, focusActive, relayWheel })
</script>

<template>
  <div class="index-wheel">
    <div class="index-focus-band" aria-hidden="true"></div>
    <div ref="viewport" class="index-wheel-viewport" tabindex="-1" @scroll.passive="schedule" @scrollend="endScroll" @wheel="scrollWheel" @touchstart.passive="endScroll" @focusin="focusRow" @keydown="keys">
      <div ref="tree" class="index-tree">
        <section v-for="group in groups" :key="group.label" class="index-group" :aria-label="group.label">
          <h3>{{ group.label }}</h3>
          <ul>
            <li v-for="item in group.items" :key="item.id">
              <div class="index-entry" :data-wheel-id="item.id" :class="{ 'is-centered': centered === item.id, 'is-parent-active': item.children?.some(child => child.id === active) }">
                <button class="index-link" :data-entry-id="item.id" :aria-current="active === item.id || (expanded !== item.id && item.children?.some(child => child.id === active)) ? 'page' : undefined" :disabled="disabled(item)" @click="emit('choose', item, $event.detail === 0)"><span class="index-text">{{ item.label }}</span></button>
                <button v-if="item.children" class="index-expand" :aria-label="(expanded === item.id ? '收起' : '展开') + item.label + '子目录'" :aria-expanded="expanded === item.id" :aria-controls="id + '-' + item.id" @click="expand(item)"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="m6 4 4 4-4 4" /></svg></button>
              </div>
              <div v-if="item.children" :id="id + '-' + item.id" class="index-children" :class="{ expanded: expanded === item.id }" :inert="expanded !== item.id" :aria-hidden="expanded !== item.id">
                <ul><li v-for="child in item.children" :key="child.id" class="index-entry" :data-wheel-id="child.id" :class="{ 'is-centered': centered === child.id }"><button class="index-link" :data-entry-id="child.id" :aria-current="active === child.id ? 'page' : undefined" :disabled="disabled(child)" @click="emit('choose', child, $event.detail === 0)"><span class="index-text">{{ child.label }}</span></button></li></ul>
              </div>
            </li>
          </ul>
        </section>
      </div>
    </div>
  </div>
</template>
