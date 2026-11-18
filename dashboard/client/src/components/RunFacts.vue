<script setup lang="ts">
import { computed } from 'vue'
import { formatNumber as fmt, text } from '../data'
import type { JsonObject } from '../types'

const props = defineProps<{ config: JsonObject; nodes?: string }>()
const facts = computed(() => [
  { label: '节点', value: props.nodes ?? fmt(props.config.node_count) },
  { label: '键数', value: fmt(props.config.key_count) },
  { label: 'V', value: fmt(props.config.virtual_nodes), title: '每个物理节点的虚拟节点数' },
  { label: 'RF', value: fmt(props.config.desired_replication_factor), title: '目标副本数' },
  { label: 'ACK', value: text(props.config.write_ack_policy), title: '写入确认策略' },
])
</script>

<template>
  <div class="dashboard-description">
    <slot><p class="dashboard-result-label">本场结果</p></slot>
    <dl class="dashboard-facts" aria-label="运行参数">
      <div v-for="item in facts" :key="item.label" :title="item.title"><dt>{{ item.label }}</dt><dd>{{ item.value }}</dd></div>
    </dl>
  </div>
</template>
