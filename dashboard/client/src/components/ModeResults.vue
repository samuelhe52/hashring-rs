<script setup lang="ts">
import { computed, ref } from 'vue'
import RecordedFields from './RecordedFields.vue'
import type { RunDetail } from '../types'
import { number, object } from '../data'

const props = defineProps<{ run: RunDetail }>()
const selected = ref('')
const records = computed(() => [
  ...Object.entries(object(props.run.summary?.measurements)).filter(([, value]) => number(object(value).target_rate) === null && number(object(value).operations_per_second) === null).map(([key, value]) => ({ key: 'measurements.' + key, value })),
  ...Object.entries(object(props.run.summary?.observations)).filter(([key, value]) => value !== null && typeof value === 'object' && !key.startsWith('idle_') && key !== 'node_pressure_after_initial_put' && number(object(value).operations_per_second) === null).map(([key, value]) => ({ key: 'observations.' + key, value })),
])
const current = computed(() => records.value.find(record => record.key === selected.value) ?? records.value[0])
</script>

<template>
  <section class="mode-records"><div class="section-heading"><div><h3>模式专有记录</h3><p>迁移、故障轮次、恢复耗时及嵌套观测，按文件中的层级查看。</p></div></div><template v-if="current"><label class="record-selector">记录<select :value="current.key" aria-label="选择模式记录" @change="selected = ($event.target as HTMLSelectElement).value"><option v-for="record in records" :key="record.key" :value="record.key">{{ record.key }}</option></select></label><RecordedFields :key="current.key" :value="current.value" /><p class="footnote">数值按原字段显示；_ms / _seconds / _us 分别为毫秒／秒／微秒。不同计时基准的字段不能直接相加，null 表示未提供或不适用。</p></template><p v-else class="empty-inline">此运行没有其他结构化观测。不会补造迁移或故障成绩。</p></section>
</template>
