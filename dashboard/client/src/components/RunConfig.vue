<script setup lang="ts">
import { computed, ref } from 'vue'
import type { RunDetail } from '../types'
import { fieldNames, formatNumber as fmt, object, text } from '../data'

const props = defineProps<{ run: RunDetail }>()
const search = ref('')
const view = defineModel<string>('view', { default: 'parameters' })
const config = computed(() => object(props.run.manifest?.config))
const build = computed(() => object(props.run.manifest?.build))
const entries = computed(() => Object.entries(config.value).filter(([key, value]) =>
  `${key} ${fieldNames[key] || ''} ${text(value)}`.toLowerCase().includes(search.value.toLowerCase())))
</script>

<template>
  <div class="config-view">
    <div v-if="run.manifest?.source_reproducible === false" class="notice warning"><p><strong>源码可复现性未满足</strong><span>结果保留；正式引用前需核对构建与运行源码。“实验成功”与“源码可复现”是两个不同的判断。</span></p></div>
    <div v-for="warning in run.warnings" :key="warning" class="notice warning">{{ warning }}</div>
    <nav class="view-switch" aria-label="配置内容"><button :aria-pressed="view === 'parameters'" @click="view = 'parameters'">实际参数</button><button :aria-pressed="view === 'build'" @click="view = 'build'">构建与环境</button></nav>
    <section v-if="view === 'parameters'" class="panel">
      <input v-model="search" class="config-search" type="search" aria-label="搜索配置字段" placeholder="搜索参数名、含义或值" />
      <div class="table-scroll"><table class="config-table"><caption class="sr-only">运行配置字段</caption><thead><tr><th>参数</th><th>说明</th><th>值</th></tr></thead><tbody><tr v-for="[key, value] in entries" :key="key"><th scope="row"><code>{{ key }}</code></th><td>{{ fieldNames[key] || '—' }}</td><td>{{ text(value) }}</td></tr><tr v-if="!entries.length"><td colspan="3">{{ search ? '没有匹配的配置字段。' : '未提供配置。' }}</td></tr></tbody></table></div>
      <p class="footnote">来自 manifest.json，保留全部字段；适用参数取决于实验模式。</p>
    </section>
    <section v-else class="panel"><dl class="config-grid"><div><dt>源码提交</dt><dd class="break-all">{{ text(build.git_commit) }}</dd></div><div><dt>构建配置</dt><dd>{{ text(build.profile) }}</dd></div><div><dt>目标平台</dt><dd>{{ text(build.target) }}</dd></div><div><dt>Rust 编译器</dt><dd>{{ text(build.rustc_version) }}</dd></div><div><dt>构建源码有改动</dt><dd>{{ text(build.git_dirty) }}</dd></div><div><dt>源码可复现</dt><dd>{{ text(run.manifest?.source_reproducible) }}</dd></div><div><dt>结束 epoch</dt><dd>{{ fmt(run.summary?.final_epoch) }}</dd></div><div><dt>数据格式版本</dt><dd>{{ fmt(run.manifest?.schema_version) }}</dd></div><div class="full-width"><dt>运行系统</dt><dd>{{ text(run.manifest?.system) }}</dd></div></dl></section>
  </div>
</template>
