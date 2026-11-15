<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import { formatNumber as fmt } from '../data'
import { categories, eventCategory, eventName, eventSummary, isKeyEvent, relativeEventTime } from '../events'
import type { RunEvent } from '../types'

const props = defineProps<{
  events: RunEvent[]; total: number; origin: number; selected?: number | null
  animate?: boolean; empty: string; compact?: boolean
}>()
const emit = defineEmits<{ jump: [event: RunEvent] }>()
const showAll = ref(false)
const scroller = ref<HTMLElement | null>(null)
const atLatest = ref(true)
const timeline = computed(() => (showAll.value ? [...props.events] : props.events.filter(isKeyEvent)).reverse())
const selected = computed(() => timeline.value.some(event => event.line === props.selected) ? props.selected : timeline.value[0]?.line)
const animateEvents = computed(() => props.animate && atLatest.value)

watch(timeline, async (events, previous) => {
  const list = scroller.value
  if (!list || list.scrollTop <= 1 || events.length <= previous.length) return
  const top = list.getBoundingClientRect().top
  const anchor = Array.from(list.querySelectorAll('li')).find(row => row.getBoundingClientRect().bottom > top + 1)
  if (!anchor) return
  const offset = anchor.getBoundingClientRect().top - top
  // 顶部插入新记录时，让正在浏览的旧记录停在原位。
  await nextTick()
  if (anchor.isConnected) list.scrollTop += anchor.getBoundingClientRect().top - list.getBoundingClientRect().top - offset
})

function goToLatest() {
  scroller.value?.scrollTo({ top: 0, behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth' })
}
</script>

<template>
  <div class="replay-feed" :class="{ 'compact-feed': compact }">
    <div class="section-heading">
      <div><h3 v-if="!compact">事件进程</h3><p>已显示 {{ fmt(timeline.length) }} / {{ fmt(total) }} 条 · 最新在上</p></div>
      <div class="event-feed-actions">
        <button v-if="!atLatest" class="table-link" @click="goToLatest">↑ 回到最新</button>
        <label v-if="total" class="checkbox-label"><input v-model="showAll" type="checkbox" />包含采样</label>
      </div>
    </div>
    <div ref="scroller" class="playback-event-scroll" role="region" aria-label="事件进程" tabindex="0"
      @scroll="atLatest = (scroller?.scrollTop ?? 0) <= 1" @wheel.stop @touchmove.stop>
      <TransitionGroup tag="ol" name="event-arrival" class="playback-events" reversed :css="animateEvents" :move-class="animateEvents ? 'event-arrival-move' : 'event-arrival-still'">
        <li v-for="event in timeline" :key="event.line">
          <button :class="{ 'event-active': event.line === selected }" :aria-current="event.line === selected ? 'true' : undefined" :data-category="eventCategory(event.event)" @click="emit('jump', event)">
            <time>{{ relativeEventTime(event.unix_ms, origin) }}</time><i aria-hidden="true"></i><span><strong>{{ eventName(event.event) }}</strong><small>{{ eventSummary(event) || categories[eventCategory(event.event)] }}</small></span>
          </button>
        </li>
      </TransitionGroup>
      <p v-if="!timeline.length" class="empty-inline">{{ empty }}</p>
    </div>
  </div>
</template>
