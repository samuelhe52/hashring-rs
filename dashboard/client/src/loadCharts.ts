import { colors, formatNumber as fmt, number } from './data'
import { nodeColor, nodePalette } from './topology.ts'
import type { Measurement, NodeLoad } from './types'

export const loadLabels: Record<string, string> = {
  owned_keys: '主节点键数',
  ops_handled: '处理操作数',
  cpu_percent: 'CPU（单核 %）',
}

export const latencyCurveMetrics = {
  p50: { label: 'p50', color: colors[1], symbol: 'circle', lineType: 'dashed' },
  p95: { label: 'p95', color: colors[4], symbol: 'triangle', lineType: 'solid' },
  p99: { label: 'p99', color: colors[0], symbol: 'diamond', lineType: 'solid' },
  p999: { label: 'p99.9', color: colors[3], symbol: 'rect', lineType: 'solid' },
  max: { label: 'max', color: colors[2], symbol: 'circle', lineType: 'dotted' },
} as const
export type LatencyCurveMetric = keyof typeof latencyCurveMetrics
export const latencyCurveMetricKeys = Object.keys(latencyCurveMetrics) as LatencyCurveMetric[]
export const defaultLatencyCurveMetrics: readonly LatencyCurveMetric[] = ['p50', 'p99']

export function visibleLatencyCurveMetrics(available: readonly LatencyCurveMetric[], selected: readonly LatencyCurveMetric[]) {
  const matches = available.filter(metric => selected.includes(metric))
  return matches.length ? matches : available.slice(0, 1)
}

const axis = {
  axisLabel: { color: '#736f65', fontSize: 11, hideOverlap: true },
  axisLine: { show: false },
  axisTick: { show: false },
  splitLine: { lineStyle: { color: '#e5e0d7', type: 'dashed' } },
}

function latencyValue(stage: Measurement, operation: string, metric: LatencyCurveMetric) {
  const stats = stage.latency_us?.[operation]
  return stats?.samples === 0 ? null : number(stats?.[metric])
}

export function availableLatencyCurveMetrics(rows: [string, Measurement][], operation: string) {
  return latencyCurveMetricKeys.filter(metric =>
    rows.some(([, stage]) => number(stage.achieved_rate) !== null && latencyValue(stage, operation, metric) !== null))
}

export function latencyCurveLabel(operation: string, metrics: readonly LatencyCurveMetric[]) {
  return `${operation.toUpperCase()} ${metrics.map(metric => latencyCurveMetrics[metric].label).join('、')} 延迟与总成功速率曲线`
}

export function latencyCurveOption(
  rows: [string, Measurement][], operation: string, selectedRate?: number | null,
  metrics = defaultLatencyCurveMetrics,
) {
  return {
    color: metrics.map(metric => latencyCurveMetrics[metric].color),
    grid: { top: 46, left: 57, right: 38, bottom: 48, outerBoundsMode: 'none' },
    tooltip: { trigger: 'axis', valueFormatter: (value: unknown) => fmt(Array.isArray(value) ? value.at(-1) : value, 2) + ' µs' },
    legend: { top: 6, right: 19, itemWidth: 16, itemHeight: 8, textStyle: { color: '#736f65', fontSize: 11 } },
    xAxis: { ...axis, type: 'value', name: '总成功速率（ops/s）', nameLocation: 'middle', nameGap: 25, min: 0, splitNumber: 4 },
    yAxis: { ...axis, type: 'value', name: '延迟（µs）', nameGap: 14, min: 0, splitNumber: 4 },
    series: metrics.map((metric, index) => {
      const style = latencyCurveMetrics[metric]
      return {
        name: operation.toUpperCase() + ' ' + style.label, type: 'line', symbol: style.symbol, symbolSize: 8,
        connectNulls: false, lineStyle: { width: 2.3, type: style.lineType },
        data: rows.filter(([, stage]) => number(stage.achieved_rate) !== null)
          .map(([, stage]) => [stage.achieved_rate, latencyValue(stage, operation, metric)])
          .sort((a, b) => (a[0] ?? 0) - (b[0] ?? 0)),
        ...(index === 0 && selectedRate != null ? {
          markLine: { silent: true, symbol: ['none', 'none'], label: { show: false },
            lineStyle: { color: '#897b6b', type: 'dashed' }, data: [{ xAxis: selectedRate }] },
        } : {}),
      }
    }),
  }
}

export function nodeLoadRows(stage?: Measurement) {
  return (Object.entries(stage?.load_balance?.per_node ?? {}) as [string, NodeLoad][])
    .sort(([a], [b]) => a.localeCompare(b, 'en', { numeric: true }))
}

export function nodeLoadValue(node: NodeLoad, metric: string) {
  return number(node?.[metric as keyof NodeLoad])
}

export function nodeLoadOption(rows: [string, NodeLoad][], metric: string) {
  const unit = metric === 'owned_keys' ? '键' : metric === 'ops_handled' ? '次' : '%'
  const maximum = Math.max(1, ...rows.map(([, node]) => nodeLoadValue(node, metric) ?? 0))
  const scale = 10 ** Math.floor(Math.log10(maximum))
  return {
    grid: { top: 12, left: 74, right: 40, bottom: 32, outerBoundsMode: 'none' },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, valueFormatter: (value: unknown) => fmt(value, 1) + ' ' + unit },
    xAxis: { ...axis, type: 'value', min: 0, max: Math.ceil(maximum * 1.18 / scale) * scale, splitNumber: 4 },
    yAxis: { ...axis, type: 'category', inverse: true, data: rows.map(([name]) => name),
      axisLabel: { ...axis.axisLabel, width: 61, overflow: 'truncate' }, splitLine: { show: false } },
    series: [{ id: 'node-load', name: loadLabels[metric], type: 'bar', barMaxWidth: 17, showBackground: true,
      backgroundStyle: { color: '#f1ede5', borderRadius: 4 },
      label: { show: true, position: 'right', color: '#4b473f', formatter: (item: { value: unknown }) => fmt(item.value, 1) },
      data: rows.map(([id, node]) => ({ value: nodeLoadValue(node, metric),
        itemStyle: { color: nodeColor(id, nodePalette), borderRadius: [0, 4, 4, 0] } })),
    }],
  }
}
