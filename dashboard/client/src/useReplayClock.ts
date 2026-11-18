import { onBeforeUnmount, onMounted, ref } from 'vue'

export function useReplayClock(bounds: () => { start: number; end: number }, restart?: () => { time: number; line: number }) {
  const cursor = ref(0)
  const boundaryLine = ref<number | null>(null)
  const playing = ref(false)
  const speed = ref(1)
  let frame = 0
  let lastFrame = 0

  function pause() {
    playing.value = false
    cancelAnimationFrame(frame)
  }
  function seek(value: number, line: number | null = null) {
    pause()
    cursor.value = Math.max(bounds().start, Math.min(value, bounds().end))
    boundaryLine.value = line
  }
  function tick(time: number) {
    if (!playing.value) return
    if (time - lastFrame >= 33) {
      const end = bounds().end
      cursor.value = Math.min(end, cursor.value + (time - lastFrame) * speed.value)
      boundaryLine.value = null
      lastFrame = time
      if (cursor.value >= end) { pause(); return }
    }
    frame = requestAnimationFrame(tick)
  }
  function toggle() {
    if (playing.value) { pause(); return }
    if (cursor.value >= bounds().end) {
      const initial = restart?.()
      seek(initial?.time ?? bounds().start, initial?.line)
    }
    playing.value = true
    lastFrame = performance.now()
    frame = requestAnimationFrame(tick)
  }
  function onVisibility() { if (document.hidden) pause() }
  onMounted(() => document.addEventListener('visibilitychange', onVisibility))
  onBeforeUnmount(() => { pause(); document.removeEventListener('visibilitychange', onVisibility) })

  return { cursor, boundaryLine, playing, speed, seek, pause, toggle }
}
