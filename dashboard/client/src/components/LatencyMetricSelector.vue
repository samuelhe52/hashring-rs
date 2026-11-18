<script setup lang="ts">
import { computed } from 'vue'
import { latencyCurveMetrics, visibleLatencyCurveMetrics, type LatencyCurveMetric } from '../loadCharts'

const props = defineProps<{ available: readonly LatencyCurveMetric[] }>()
const selected = defineModel<LatencyCurveMetric[]>({ required: true })
const visible = computed(() => visibleLatencyCurveMetrics(props.available, selected.value))

function toggle(metric: LatencyCurveMetric) {
  if (!visible.value.includes(metric)) selected.value = [...visible.value, metric]
  else if (visible.value.length > 1) selected.value = visible.value.filter(value => value !== metric)
}
</script>

<template>
  <div v-if="available.length" class="dashboard-card-tabs dashboard-curve-metrics" role="group" aria-label="曲线分位数">
    <button v-for="metric in available" :key="metric" type="button" :aria-pressed="visible.includes(metric)"
      :aria-disabled="visible.length === 1 && visible.includes(metric) ? true : undefined"
      :title="metric === 'max' ? '最大延迟（直方图桶上界）' : undefined" @click="toggle(metric)">
      <span class="curve-metric-color" :style="{ backgroundColor: latencyCurveMetrics[metric].color }" aria-hidden="true"></span>
      {{ latencyCurveMetrics[metric].label }}
    </button>
  </div>
</template>
