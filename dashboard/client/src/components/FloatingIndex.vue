<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, useId, watch } from 'vue'
import DirectoryTree from './DirectoryTree.vue'
import type { DirectoryEntry, DirectoryGroup } from '../navigation'

const props = defineProps<{ groups: DirectoryGroup[]; active: string; hasRun: boolean }>()
const emit = defineEmits<{ navigate: [entry: DirectoryEntry, keyboard: boolean] }>()
const root = ref<HTMLElement | null>(null)
const menu = ref<HTMLDialogElement | null>(null)
const toggle = ref<HTMLButtonElement | null>(null)
const railTree = ref<InstanceType<typeof DirectoryTree> | null>(null)
const dialogTree = ref<InstanceType<typeof DirectoryTree> | null>(null)
const open = ref(false)
const closing = ref(false)
const instant = ref(false)
const progress = ref(0)
const scrolledEntry = ref('')
const menuId = useId()
const headingId = useId()
const parents = computed(() => props.groups.flatMap(group => group.items))
const activeParent = computed(() => parents.value.find(item => item.id === props.active || item.children?.some(child => child.id === props.active)))
const expanded = ref(activeParent.value?.children ? activeParent.value.id : '')
const current = computed(() => scrolledEntry.value || props.active)
let scroller: HTMLElement | null = null
let resizeObserver: ResizeObserver | undefined
let closeTimer: ReturnType<typeof setTimeout> | undefined
let frame = 0
let outsidePress = false
let locked: { element: HTMLElement; overflow: string } | null = null

function schedule() { if (!frame) frame = requestAnimationFrame(update) }
function update() {
  frame = 0
  const area = scroller ?? document.scrollingElement
  if (!area) return
  const distance = area.scrollHeight - area.clientHeight
  progress.value = distance > 0 ? Math.max(0, Math.min(1, area.scrollTop / distance)) : 1
  if (area.scrollTop < 2) { scrolledEntry.value = ''; return }
  const top = scroller?.getBoundingClientRect().top ?? 0
  const reached = activeParent.value?.children?.filter(entry => {
    const section = entry.anchor ? document.getElementById(entry.anchor) : null
    return section && section.getBoundingClientRect().top <= top + 85
  })
  scrolledEntry.value = reached?.at(-1)?.id ?? ''
}

function releaseScroll() {
  if (!locked) return
  locked.element.style.overflow = locked.overflow
  locked = null
}

function closeMenu(afterClose?: () => void, keyboard = false) {
  if (!menu.value?.open || closing.value) return
  instant.value = keyboard || matchMedia('(prefers-reduced-motion: reduce)').matches
  closing.value = true
  const finish = () => {
    menu.value?.close()
    open.value = false
    closing.value = false
    releaseScroll()
    afterClose?.()
  }
  closeTimer = setTimeout(finish, instant.value ? 0 : 180)
}

async function openMenu(event: MouseEvent) {
  if (!menu.value || menu.value.open) return
  instant.value = event.detail === 0
  const element = scroller ?? document.body
  locked = { element, overflow: element.style.overflow }
  element.style.overflow = 'hidden'
  open.value = true
  closing.value = false
  menu.value.showModal()
  await nextTick()
  const nav = menu.value.querySelector<HTMLElement>('.index-dialog-nav')
  dialogTree.value?.centerActive()
  if (event.detail === 0) dialogTree.value?.focusActive()
  else nav?.focus({ preventScroll: true })
}

function choose(entry: DirectoryEntry, keyboard: boolean) {
  const navigate = () => emit('navigate', entry, keyboard)
  if (menu.value?.open) closeMenu(navigate, keyboard)
  else navigate()
}

function outside(event: MouseEvent) {
  if (!menu.value || event.target !== menu.value) return false
  const box = menu.value.getBoundingClientRect()
  return event.clientX < box.left || event.clientX > box.right || event.clientY < box.top || event.clientY > box.bottom
}

function rememberPress(event: PointerEvent) { outsidePress = outside(event) }

function keepFocus(event: KeyboardEvent) {
  if (event.key !== 'Tab' || !menu.value) return
  const buttons = [...menu.value.querySelectorAll<HTMLButtonElement>('button:not(:disabled)')]
    .filter(button => !button.closest('[inert]') && button.checkVisibility())
  const first = buttons[0]
  const last = buttons.at(-1)
  if (event.shiftKey && document.activeElement === first && last) { event.preventDefault(); last.focus() }
  else if (!event.shiftKey && document.activeElement === last && first) { event.preventDefault(); first.focus() }
}

function resize() {
  if (menu.value?.open && toggle.value && !toggle.value.checkVisibility()) closeMenu()
  schedule()
}

watch(() => props.active, () => {
  scrolledEntry.value = ''
  expanded.value = activeParent.value?.children ? activeParent.value.id : ''
}, { flush: 'post' })

onMounted(() => {
  scroller = root.value?.closest('dialog.detail-window')?.querySelector<HTMLElement>('.window-body') ?? null
  const target = scroller ?? window
  target.addEventListener('scroll', schedule, { passive: true })
  window.addEventListener('resize', resize)
  const content = scroller ?? document.querySelector('.dashboard-main')
  if (content) { resizeObserver = new ResizeObserver(schedule); resizeObserver.observe(content) }
  schedule()
})
onBeforeUnmount(() => {
  const target = scroller ?? window
  target.removeEventListener('scroll', schedule)
  window.removeEventListener('resize', resize)
  resizeObserver?.disconnect()
  cancelAnimationFrame(frame)
  clearTimeout(closeTimer)
  menu.value?.close()
  releaseScroll()
})
</script>

<template>
  <div ref="root" class="floating-index">
    <aside class="index-rail" aria-label="Dashboard 目录" @wheel="railTree?.relayWheel($event)">
      <div class="index-rail-heading"><span>目录</span></div>
      <nav class="index-rail-nav" aria-label="完整目录">
        <DirectoryTree ref="railTree" :groups="groups" :active="current" :expanded="expanded" :has-run="hasRun" @expand="expanded = $event" @choose="choose" />
      </nav>
      <p class="index-wheel-hint"><span class="index-mouse-hint">滚动浏览</span><span class="index-touch-hint">上下滑动</span><span>点击打开</span></p>
    </aside>

    <button ref="toggle" type="button" class="index-toggle" title="打开完整目录" aria-label="打开完整目录" aria-haspopup="dialog" :aria-expanded="open" :aria-controls="menuId" @click="openMenu">
      <svg class="index-progress" viewBox="0 0 48 48" aria-hidden="true"><circle class="index-progress-track" cx="24" cy="24" r="22" /><circle class="index-progress-value" cx="24" cy="24" r="22" pathLength="1" stroke-dasharray="1" :stroke-dashoffset="1 - progress" /></svg>
      <svg class="index-icon" viewBox="0 0 20 20" aria-hidden="true"><path d="M7 5h9M7 10h9M7 15h6M3.5 5h.1M3.5 10h.1M3.5 15h.1" /></svg>
    </button>

    <dialog :id="menuId" ref="menu" class="index-dialog" :class="{ closing, instant }" :aria-labelledby="headingId" @cancel.stop.prevent="closeMenu(undefined, true)"
      @wheel="dialogTree?.relayWheel($event)" @keydown="keepFocus" @pointerdown="rememberPress" @click="outsidePress && outside($event) && closeMenu()">
      <header class="index-dialog-heading"><h2 :id="headingId">目录</h2><button class="index-close" aria-label="关闭目录" @click="closeMenu(undefined, $event.detail === 0)"><svg viewBox="0 0 20 20" aria-hidden="true"><path d="m5 5 10 10m0-10L5 15" /></svg></button></header>
      <nav class="index-dialog-nav" aria-label="完整目录" tabindex="-1"><DirectoryTree ref="dialogTree" :groups="groups" :active="current" :expanded="expanded" :has-run="hasRun" @expand="expanded = $event" @choose="choose" /></nav>
      <p class="index-wheel-hint"><span class="index-mouse-hint">滚动浏览</span><span class="index-touch-hint">上下滑动</span><span>点击打开</span></p>
    </dialog>
  </div>
</template>
