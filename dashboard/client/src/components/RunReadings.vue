<script setup lang="ts">
defineProps<{ items: { label: string; value: string; unit: string; note: string; tone: string }[]; label: string }>()
</script>

<template>
  <dl class="dashboard-readings" :aria-label="label">
    <div v-for="reading in items" :key="reading.label" :data-tone="reading.tone">
      <dt>{{ reading.label }}</dt>
      <dd>
        <strong :class="{ 'missing-reading': reading.value === '未记录', 'long-reading': reading.value.length > 9 }">{{ reading.value }}</strong>
        <span v-if="reading.value !== '未记录' && reading.unit">{{ reading.unit }}</span>
      </dd>
      <p>{{ reading.note }}</p>
    </div>
  </dl>
</template>
