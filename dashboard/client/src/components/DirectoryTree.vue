<script setup lang="ts">
import { ref, useId } from 'vue'
import type { DirectoryEntry, DirectoryGroup } from '../navigation'

const props = defineProps<{ groups: DirectoryGroup[]; active: string; expanded: string; hasRun: boolean }>()
const emit = defineEmits<{ choose: [entry: DirectoryEntry, keyboard: boolean]; expand: [id: string] }>()
const tree = ref<HTMLElement | null>(null)
const id = useId()

function disabled(entry: DirectoryEntry) {
  return !props.hasRun && entry.panel !== null && !['runs', 'experiment', 'reference'].includes(entry.panel)
}

function enabledLinks() {
  return [...(tree.value?.querySelectorAll<HTMLButtonElement>('.index-link:not(:disabled)') ?? [])]
    .filter(link => !link.closest('[hidden]'))
}

function focusActive() {
  const links = enabledLinks()
  const current = links.find(link => link.getAttribute('aria-current') === 'page') ?? links[0]
  current?.focus()
}

function keys(event: KeyboardEvent) {
  if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return
  const directions: Record<string, number> = { ArrowUp: -1, ArrowDown: 1, Home: -Infinity, End: Infinity }
  const direction = directions[event.key]
  if (direction === undefined) return
  const links = enabledLinks()
  const current = (event.target as HTMLElement).closest('.index-entry')?.querySelector('.index-link')
  const index = links.findIndex(link => link === current)
  const next = links[Math.max(0, Math.min(links.length - 1, index + direction))]
  if (!next) return
  event.preventDefault()
  event.stopPropagation()
  next.focus()
}

defineExpose({ focusActive })
</script>

<template>
  <div ref="tree" class="index-tree" @keydown="keys">
    <section v-for="group in groups" :key="group.label" class="index-group" :aria-label="group.label">
      <h3>{{ group.label }}</h3>
      <ul>
        <li v-for="item in group.items" :key="item.id">
          <div class="index-entry">
            <button type="button" class="index-link" :data-entry-id="item.id" :disabled="disabled(item)"
              :aria-current="active === item.id || (expanded !== item.id && item.children?.some(child => child.id === active)) ? 'page' : undefined"
              @click="emit('choose', item, $event.detail === 0)">{{ item.label }}</button>
            <button v-if="item.children" type="button" class="index-expand" :disabled="disabled(item)"
              :aria-label="(expanded === item.id ? '收起' : '展开') + item.label + '子目录'" :aria-expanded="expanded === item.id" :aria-controls="id + '-' + item.id"
              @click="emit('expand', expanded === item.id ? '' : item.id)"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="m6 4 4 4-4 4" /></svg></button>
          </div>
          <ul v-if="item.children" :id="id + '-' + item.id" class="index-children" :hidden="expanded !== item.id">
            <li v-for="child in item.children" :key="child.id" class="index-entry">
              <button type="button" class="index-link" :data-entry-id="child.id" :aria-current="active === child.id ? 'page' : undefined"
                :disabled="disabled(child)" @click="emit('choose', child, $event.detail === 0)">{{ child.label }}</button>
            </li>
          </ul>
        </li>
      </ul>
    </section>
  </div>
</template>
