<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue'
import TopologyView from './TopologyView.vue'
import type { EventLog, RunDetail } from '../types'
import { request } from '../data'

const props = defineProps<{ run: RunDetail; initialLine?: number }>()
const view = defineModel<string>('view', { default: 'range' })
const log = ref<EventLog>({ events: [], issues: [], missing: false })
const loading = ref(true)
const error = ref('')
let controller: AbortController | undefined
async function load() {
  controller?.abort()
  controller = new AbortController()
  const signal = controller.signal
  loading.value = true
  error.value = ''
  try { log.value = await request<EventLog>(`/api/runs/${encodeURIComponent(props.run.id)}/events`, signal) }
  catch (failure) { if (!signal.aborted) error.value = failure instanceof Error ? failure.message : String(failure) }
  finally { if (!signal.aborted) loading.value = false }
}
onMounted(load)
onBeforeUnmount(() => controller?.abort())
</script>

<template>
  <p v-if="loading" class="empty-inline" role="status">正在读取拓扑快照……</p>
  <div v-else-if="error" class="notice error" role="alert"><p>{{ error }}</p><button class="table-link" @click="load">重试</button></div>
  <template v-else><div v-if="log.issues.length" class="notice warning">{{ log.issues.length }} 行记录无法解析；仅使用可读快照。问题原文可在事件时间线核对。</div><TopologyView v-model:view="view" :run="run" :events="log.events" :initial-line="initialLine" /></template>
</template>
