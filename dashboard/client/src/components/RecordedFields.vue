<script setup lang="ts">
import { computed, ref } from 'vue'
import { fieldNames, text } from '../data'

const props = defineProps<{ value: unknown }>()
const page = ref(1)
const open = ref<Record<string, boolean>>({})
const entries = computed(() => Array.isArray(props.value) ? props.value.map((value, index) => [String(index + 1), value] as const) : Object.entries(props.value && typeof props.value === 'object' ? props.value : {}))
const visible = computed(() => entries.value.slice((page.value - 1) * 20, page.value * 20))
const labels: Record<string, string> = { ...fieldNames, fault_rounds: '故障轮次', availability_rounds: '可用性轮次', scaling_metrics: '扩缩容分阶段指标', published_ms: '发布耗时（ms）', complete_ms: '变更完成耗时（ms）', full_rf_seconds: 'Complete 后恢复完整 RF（s）', stop_window: '停写窗口', migration: '迁移记录', work_steps_ms: '各步骤耗时（ms）', round: '轮次', final_epoch: '结束 epoch', victim: '故障节点', success: '成功', error: '错误', dropped_acknowledged_states: '丢弃的已确认状态数', may_have_applied: '可能已生效', impossible_reads: '不可解释的读取', inconclusive_reads: '无法判定的读取' }
const nested = (value: unknown) => value !== null && typeof value === 'object'
</script>

<template>
  <div class="recorded-fields">
    <template v-for="[key, value] in visible" :key="key">
      <details v-if="nested(value)" class="record-branch" @toggle="open[key] = ($event.target as HTMLDetailsElement).open"><summary><span>{{ labels[key] || key }}</span><code v-if="labels[key]">{{ key }}</code><small>{{ Array.isArray(value) ? value.length + ' 项' : Object.keys(value as object).length + ' 个字段' }}</small></summary><RecordedFields v-if="open[key]" :value="value" /></details>
      <dl v-else class="record-leaf"><dt><span>{{ labels[key] || key }}</span><code v-if="labels[key]">{{ key }}</code></dt><dd>{{ value === null || value === undefined ? '—' : text(value) }}</dd></dl>
    </template>
    <p v-if="!entries.length" class="footnote">没有记录字段。</p>
    <div v-if="entries.length > 20" class="event-pagination"><button class="button secondary" :disabled="page === 1" @click="page--">上一页</button><span>{{ page }} / {{ Math.ceil(entries.length / 20) }}</span><button class="button secondary" :disabled="page * 20 >= entries.length" @click="page++">下一页</button></div>
  </div>
</template>
