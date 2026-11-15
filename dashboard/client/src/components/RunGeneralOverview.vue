<script setup lang="ts">
import { computed } from 'vue'
import RunFacts from './RunFacts.vue'
import RunReadings from './RunReadings.vue'
import { fieldNames, formatNumber as fmt, number, object, throughputRows } from '../data'
import type { RunDetail } from '../types'

const props = defineProps<{ run: RunDetail }>()
const emit = defineEmits<{ observations: [view: 'throughput' | 'idle'] }>()
const throughput = computed(() => throughputRows(props.run.summary))
const observations = computed(() => object(props.run.summary?.observations))
const idle = computed(() => [
  { path: '客户端 PUT', stats: object(observations.value.idle_client_put_latency_us ?? observations.value.client_put_latency_us) },
  { path: '直连 Owner RPC PUT', stats: object(observations.value.idle_owner_rpc_put_latency_us ?? observations.value.owner_rpc_put_latency_us) },
].filter(row => Object.keys(row.stats).length))
const readings = computed(() => {
  const summary = props.run.summary
  const initial = throughput.value.find(([name]) => name === 'initial_put')?.[1]
  return [
    { label: '实验总耗时', value: reading(summary?.elapsed_seconds), unit: 's', note: '实验程序记录', tone: 'neutral' },
    { label: '初始写入吞吐', value: reading(initial?.operations_per_second), unit: 'ops/s', note: '预加载阶段', tone: 'ok' },
    { label: '最终 epoch', value: reading(summary?.final_epoch), unit: '', note: '结束时的拓扑版本', tone: 'neutral' },
  ]
})
function reading(value: unknown) { return number(value) === null ? '未记录' : fmt(value, 2) }
</script>

<template>
  <section class="dashboard-content" aria-label="实验观测概览">
    <RunFacts :config="object(run.manifest?.config)" />
    <RunReadings :items="readings" label="本场关键读数" />
    <div class="dashboard-pair">
      <section class="dashboard-card">
        <header class="dashboard-card-heading"><h3>批量吞吐</h3></header>
        <div v-if="throughput.length" class="table-scroll dashboard-table-body">
          <table>
            <thead><tr><th scope="col">步骤</th><th scope="col">操作数</th><th scope="col">耗时 s</th><th scope="col">ops/s</th></tr></thead>
            <tbody><tr v-for="[name, row] in throughput" :key="name">
              <th scope="row">{{ fieldNames[name] ?? name }}</th><td>{{ fmt(row.operations) }}</td><td>{{ fmt(row.seconds, 3) }}</td><td>{{ fmt(row.operations_per_second, 2) }}</td>
            </tr></tbody>
          </table>
        </div>
        <p v-else class="empty-inline dashboard-table-body">本场未记录批量吞吐。</p>
        <footer class="dashboard-card-footer"><span></span><button class="table-link" aria-haspopup="dialog" @click="emit('observations', 'throughput')">完整观测 ↗</button></footer>
      </section>
      <section class="dashboard-card">
        <header class="dashboard-card-heading"><h3>空闲基线</h3><span class="dashboard-card-note">PUT 延迟 · µs</span></header>
        <div v-if="idle.length" class="table-scroll dashboard-table-body">
          <table>
            <thead><tr><th scope="col">路径</th><th scope="col">p50</th><th scope="col">p95</th><th scope="col">p99</th></tr></thead>
            <tbody><tr v-for="row in idle" :key="row.path">
              <th scope="row">{{ row.path }}</th><td>{{ fmt(row.stats.p50) }}</td><td>{{ fmt(row.stats.p95) }}</td><td>{{ fmt(row.stats.p99) }}</td>
            </tr></tbody>
          </table>
        </div>
        <p v-else class="empty-inline dashboard-table-body">本场未记录空闲基线。</p>
        <footer class="dashboard-card-footer"><span></span><button class="table-link" aria-haspopup="dialog" @click="emit('observations', 'idle')">基线图表 ↗</button></footer>
      </section>
    </div>
  </section>
</template>
