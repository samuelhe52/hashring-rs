<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'

const props = defineProps<{ title: string; subtitle?: string; compact?: boolean; wide?: boolean; pageKey?: string }>()
const emit = defineEmits<{ close: [] }>()
const dialog = ref<HTMLDialogElement | null>(null)
const closing = ref(false)
const ready = ref(false)
let trigger: HTMLElement | null = null
let previousOverflow = ''
let closeTimer: ReturnType<typeof setTimeout> | undefined
let startedOutside = false

function dismiss() {
  if (closing.value) return
  closing.value = true
  closeTimer = setTimeout(() => emit('close'), matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 160)
}

function isOutside(event: MouseEvent | PointerEvent) {
  if (event.target !== dialog.value || !dialog.value) return false
  const rect = dialog.value.getBoundingClientRect()
  return event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom
}

function pointerDown(event: PointerEvent) { startedOutside = isOutside(event) }

watch(() => props.pageKey, () => {
  dialog.value?.querySelector('.window-body')?.scrollTo({ top: 0, behavior: 'instant' })
}, { flush: 'post' })

function keepFocus(event: KeyboardEvent) {
  if (event.key !== 'Tab' || !dialog.value) return
  if (event.target instanceof Element && event.target.closest('dialog') !== dialog.value) return
  const controls = [...dialog.value.querySelectorAll<HTMLElement>('a[href], button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), summary, [tabindex]:not([tabindex="-1"])')]
    .filter(element => element.closest('dialog') === dialog.value && !element.closest('[inert]') && element.tabIndex >= 0 && element.checkVisibility())
  const first = controls[0]
  const last = controls.at(-1)
  if (event.shiftKey && document.activeElement === first && last) { event.preventDefault(); last.focus() }
  else if (!event.shiftKey && document.activeElement === last && first) { event.preventDefault(); first.focus() }
}

onMounted(() => {
  trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null
  previousOverflow = document.body.style.overflow
  document.body.style.overflow = 'hidden'
  if (trigger && dialog.value) {
    const box = trigger.getBoundingClientRect()
    const x = Math.max(-18, Math.min(18, (box.left + box.width / 2 - innerWidth / 2) * .065))
    const y = Math.max(-18, Math.min(18, (box.top + box.height / 2 - innerHeight / 2) * .065))
    dialog.value.style.setProperty('--window-x', x + 'px')
    dialog.value.style.setProperty('--window-y', y + 'px')
  }
  dialog.value?.showModal()
  // 子图表要在窗口可见、有实际尺寸以后初始化。
  ready.value = true
})
onBeforeUnmount(() => {
  clearTimeout(closeTimer)
  dialog.value?.close()
  document.body.style.overflow = previousOverflow
  nextTick(() => {
    if (trigger?.isConnected && trigger !== document.body) trigger.focus({ preventScroll: true })
    else {
      const toggle = document.querySelector<HTMLElement>('.index-toggle')
      const fallback = toggle?.checkVisibility() ? toggle : document.querySelector<HTMLElement>('[data-entry-id="overview"]')
      fallback?.focus({ preventScroll: true })
    }
  })
})
</script>

<template>
  <dialog ref="dialog" class="detail-window" :class="{ closing, compact, wide }" aria-labelledby="window-title" :aria-describedby="subtitle ? 'window-subtitle' : undefined"
    @cancel.prevent="dismiss" @keydown="keepFocus" @pointerdown="pointerDown" @click="startedOutside && isOutside($event) && dismiss()">
    <div class="window-frame">
      <header class="window-heading"><div><h2 id="window-title" tabindex="-1">{{ title }}</h2><p v-if="subtitle" id="window-subtitle">{{ subtitle }}</p></div><button class="close-window" aria-label="关闭详情" autofocus @click="dismiss"><svg viewBox="0 0 20 20" aria-hidden="true"><path d="m5 5 10 10M15 5 5 15" /></svg></button></header>
      <div v-if="ready" class="window-workspace">
        <div class="window-body"><div :key="pageKey" class="window-content"><slot /></div></div>
        <div class="window-navigation"><slot name="navigation" /></div>
      </div>
      <footer class="window-footer"><span>按 Esc 或点击窗口外返回</span></footer>
    </div>
  </dialog>
</template>
