<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { request, requestText } from '../data'

const props = defineProps<{ runId: string; kind: 'raw' | 'logs' }>()
const names = ref(props.kind === 'raw' ? ['summary.json', 'manifest.json', 'events.jsonl'] : [] as string[])
const selected = ref(props.kind === 'raw' ? 'summary.json' : '')
const content = ref('')
const query = ref('')
const loading = ref(false)
const error = ref('')
let controller: AbortController | undefined
const base = computed(() => `/api/runs/${encodeURIComponent(props.runId)}`)
const fileUrl = computed(() => selected.value ? props.kind === 'raw'
  ? `${base.value}/raw?file=${encodeURIComponent(selected.value)}`
  : `${base.value}/logs?process=${encodeURIComponent(selected.value)}` : '')
const visible = computed(() => query.value
  ? content.value.split('\n').filter(line => line.toLowerCase().includes(query.value.toLowerCase())).join('\n')
  : content.value)

async function loadFile() {
  controller?.abort()
  controller = new AbortController()
  const signal = controller.signal
  content.value = ''
  error.value = ''
  query.value = ''
  if (!fileUrl.value) return
  loading.value = true
  try { content.value = await requestText(fileUrl.value, signal) }
  catch (failure) { if (!signal.aborted) error.value = failure instanceof Error ? failure.message : String(failure) }
  finally { if (!signal.aborted) loading.value = false }
}
watch(selected, loadFile)
onMounted(async () => {
  if (props.kind === 'raw') return loadFile()
  controller = new AbortController()
  const signal = controller.signal
  loading.value = true
  try {
    names.value = await request<string[]>(`${base.value}/logs`, signal)
    selected.value = names.value[0] ?? ''
  } catch (failure) { if (!signal.aborted) error.value = failure instanceof Error ? failure.message : String(failure) }
  finally { if (!signal.aborted) loading.value = false }
})
onBeforeUnmount(() => controller?.abort())
</script>

<template>
  <section class="panel file-panel">
    <div class="section-heading"><h3>{{ kind === 'raw' ? '结果文件' : '进程日志' }}</h3>
      <a v-if="selected && !error && !loading" class="button secondary" :href="fileUrl + '&download=1'" :download="selected">下载文件</a>
    </div>
    <div class="file-toolbar">
      <label>文件<select v-model="selected" aria-label="选择文件" :disabled="!names.length"><option v-for="name in names" :key="name" :value="name">{{ name }}</option></select></label>
      <label class="file-search">搜索行<input v-model="query" type="search" placeholder="输入要查找的内容" :disabled="loading || !content" /></label>
    </div>
    <div v-if="loading" class="empty-inline" role="status">正在读取文件……</div>
    <div v-else-if="error" class="notice error" role="alert"><p>{{ error }}</p><button class="table-link" @click="loadFile">重新读取</button></div>
    <p v-else-if="!names.length" class="empty-inline">此运行没有可查看的日志文件。</p>
    <template v-else>
      <p v-if="query" class="footnote">仅显示包含“{{ query }}”的行。下载包含文件的全部内容。</p>
      <pre v-if="visible" class="source-view" tabindex="0" :aria-label="selected + '原文'">{{ visible }}</pre>
      <p v-else class="empty-inline">{{ query ? '没有匹配的行。' : '文件为空。' }}</p>
    </template>
  </section>
</template>
