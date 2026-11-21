<script setup lang="ts">
import { onMounted, onBeforeUnmount, ref, watch } from 'vue'
import { init, use, type EChartsCoreOption, type EChartsType } from 'echarts/core'
import { BarChart, LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent, AriaComponent, MarkAreaComponent, MarkLineComponent, DataZoomSliderComponent } from 'echarts/components'
import { SVGRenderer } from 'echarts/renderers'
import { object } from '../data'
import { downloadBlob, svgToPng } from '../download'

use([BarChart, LineChart, GridComponent, LegendComponent, TooltipComponent, AriaComponent, MarkAreaComponent, MarkLineComponent, DataZoomSliderComponent, SVGRenderer])
const props = withDefaults(defineProps<{ option: EChartsCoreOption; label: string; animateUpdates?: boolean; motion?: boolean; exportable?: boolean; source?: string }>(), { motion: true, animateUpdates: false })
const emit = defineEmits<{ layout: []; zoom: [range: { start: number; end: number }] }>()
const container = ref<HTMLDivElement | null>(null)
const exportError = ref('')
const exporting = ref(false)
let chart: EChartsType | undefined
let resizeObserver: ResizeObserver | undefined
let painted = false
const chartText = { fontFamily: 'system-ui, Microsoft YaHei, sans-serif', color: '#736f65' }

function update() {
  const first = !painted
  const interpolate = !first && props.animateUpdates
  // Replay updates the series as the cursor moves. Keep the slider's selected range.
  const merge = props.option.dataZoom ? { replaceMerge: ['series', 'dataZoom'] }
    : interpolate ? { replaceMerge: ['series'] } : { notMerge: true }
  chart?.setOption({
    backgroundColor: 'transparent',
    textStyle: chartText,
    aria: { enabled: true, description: props.label },
    ...props.option,
    animation: props.motion !== false && !matchMedia('(prefers-reduced-motion: reduce)').matches && (first || props.animateUpdates === true),
    animationDuration: 540,
    animationDurationUpdate: interpolate ? 360 : 0,
    animationDelay: (index: number) => first ? Math.min(index * 40, 200) : 0,
    animationEasing: 'cubicOut',
    animationEasingUpdate: 'cubicOut',
    animationDelayUpdate: (index: number) => Math.min(index * 28, 112),
    tooltip: { confine: true, ...object(props.option.tooltip) },
  }, merge)
  painted = true
  emit('layout')
}

async function exportChart(format: 'svg' | 'png') {
  exporting.value = true
  exportError.value = ''
  const width = 1100
  const chartHeight = 416
  let figure: EChartsType | undefined
  try {
    const measurement = document.createElement('canvas').getContext('2d')
    if (!measurement) throw new Error('无法测量导出图的文字')
    function wrapCaption(value: string, size: number) {
      measurement!.font = size + 'px ' + chartText.fontFamily
      const rows: string[] = []
      let row = ''
      for (const character of value) {
        if (measurement!.measureText(row + character).width > width - 48 && row) {
          rows.push(row.trimEnd())
          row = ''
        }
        row += character
      }
      rows.push(row.trim())
      return rows
    }
    const title = wrapCaption(props.label, 22)
    const source = wrapCaption(props.source ? 'hashring-rs / ' + props.source : 'hashring-rs 实验 Dashboard', 13)
    const chartTop = 54 + (title.length - 1) * 28
    const height = chartTop + chartHeight + 50 + (source.length - 1) * 19
    figure = init(null, undefined, { renderer: 'svg', ssr: true, width, height: chartHeight })
    const series = props.option.series
    const showLegend = !props.option.legend && Array.isArray(series) && series.length > 1
    const grid = object(props.option.grid)
    figure.setOption({
      textStyle: { ...chartText, fontSize: 13 },
      ...props.option,
      ...(showLegend ? { legend: { bottom: 2 }, grid: { ...grid, bottom: (typeof grid.bottom === 'number' ? grid.bottom : 40) + 28 } } : {}),
      backgroundColor: 'transparent',
      animation: false,
    })
    const svg = new DOMParser().parseFromString(figure.renderToSVGString(), 'image/svg+xml').documentElement
    const ns = 'http://www.w3.org/2000/svg'
    const sheet = document.createElementNS(ns, 'svg')
    sheet.setAttribute('width', String(width))
    sheet.setAttribute('height', String(height))
    sheet.setAttribute('viewBox', `0 0 ${width} ${height}`)
    const paper = document.createElementNS(ns, 'rect')
    paper.setAttribute('width', '100%')
    paper.setAttribute('height', '100%')
    paper.setAttribute('fill', '#fbfaf7')
    sheet.append(paper)
    function caption(rows: string[], y: number, size: number, spacing: number, color: string) {
      rows.forEach((value, index) => {
        const line = document.createElementNS(ns, 'text')
        line.setAttribute('x', '24')
        line.setAttribute('y', String(y + index * spacing))
        line.setAttribute('fill', color)
        line.setAttribute('font-size', String(size))
        line.setAttribute('font-family', chartText.fontFamily)
        line.textContent = value
        sheet.append(line)
      })
    }
    caption(title, 35, 22, 28, '#282720')
    svg.setAttribute('y', String(chartTop))
    sheet.append(svg)
    caption(source, chartTop + chartHeight + 32, 13, 19, '#736f65')
    const blob = new Blob([new XMLSerializer().serializeToString(sheet)], { type: 'image/svg+xml;charset=utf-8' })
    const file = props.label.replace(/[\\/:*?"<>|]/g, '-').slice(0, 100) + '.' + format
    downloadBlob(format === 'svg' ? blob : await svgToPng(blob, width, height), file)
  } catch (error) {
    exportError.value = error instanceof Error ? error.message : '图表导出失败'
  } finally {
    figure?.dispose()
    exporting.value = false
  }
}

onMounted(() => {
  if (!container.value) return
  chart = init(container.value, undefined, { renderer: 'svg' })
  chart.on('datazoom', event => {
    const { start, end } = object(event)
    if (typeof start === 'number' && typeof end === 'number') emit('zoom', { start, end })
    emit('layout')
  })
  update()
  let width = container.value.clientWidth
  let height = container.value.clientHeight
  resizeObserver = new ResizeObserver(() => {
    if (!container.value) return
    const nextWidth = container.value.clientWidth
    const nextHeight = container.value.clientHeight
    if (nextWidth !== width || nextHeight !== height) { width = nextWidth; height = nextHeight; chart?.resize(); emit('layout') }
  })
  resizeObserver.observe(container.value)
})
watch(() => props.option, update, { deep: true })
onBeforeUnmount(() => { resizeObserver?.disconnect(); chart?.dispose() })
defineExpose({
  pixelAt(value: number): number | null {
    const pixel = chart?.convertToPixel({ xAxisIndex: 0 }, value)
    return typeof pixel === 'number' && Number.isFinite(pixel) ? pixel : null
  },
  resetZoom() { chart?.dispatchAction({ type: 'dataZoom', start: 0, end: 100 }) },
})
</script>

<template>
  <div v-if="exportable" class="chart-export">
    <span>导出图表</span>
    <button class="button secondary" :disabled="exporting" @click="exportChart('png')">PNG</button>
    <button class="button secondary" :disabled="exporting" @click="exportChart('svg')">SVG</button>
    <p v-if="exportError" class="text-warning" role="alert">{{ exportError }}</p>
  </div>
  <div ref="container" class="data-chart" role="img" :aria-label="label"></div>
</template>
