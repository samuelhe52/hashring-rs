import type { RunDetail, JsonObject } from './types'
import { number, object, stages } from './data.ts'

// 对象字段顺序不构成配置差异；undefined（缺字段）与 null 分开。
export function canonical(value: unknown): string {
  if (value === undefined) return '<missing>'
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']'
  if (value && typeof value === 'object') return '{' + Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, item]) => JSON.stringify(key) + ':' + canonical(item)).join(',') + '}'
  return JSON.stringify(value)
}

export function configRows(runs: RunDetail[]) {
  const configs = runs.map(run => object(run.manifest?.config))
  return [...new Set(configs.flatMap(config => Object.keys(config)))].sort().map(key => {
    const values = configs.map(config => config[key])
    return { key, values, different: new Set(values.map(canonical)).size > 1 }
  })
}

export function comparisonRates(runs: RunDetail[]): number[] {
  return [...new Set(runs.flatMap(run => stages(run.summary).map(([, value]) => value.target_rate as number)))].sort((a, b) => a - b)
}

export function stagesAtRate(run: RunDetail, rate: number | null) {
  return stages(run.summary).filter(([, value]) => value.target_rate === rate)
}

// 只列出已记录的有限数值，既不把 null 变成 0，也不聚合分位数。
export function numericFields(value: unknown, prefix = ''): Map<string, number> {
  const fields = new Map<string, number>()
  if (number(value) !== null) { fields.set(prefix, value as number); return fields }
  const entries = Array.isArray(value) ? value.map((item, index) => [String(index), item] as const) : Object.entries(object(value))
  for (const [key, item] of entries) {
    const path = prefix ? prefix + '.' + key : key
    for (const [child, scalar] of numericFields(item, path)) fields.set(child, scalar)
  }
  return fields
}

export function recordedMetrics(run: RunDetail): JsonObject {
  return { measurements: object(run.summary?.measurements), observations: object(run.summary?.observations) }
}

export function sameExperimentConfig(runs: RunDetail[]): boolean {
  if (runs.length < 2 || runs.some(run => run.status !== 'success' || !run.manifest?.config?.mode)) return false
  return !configRows(runs).some(row => row.key !== 'output_dir' && row.different)
}

export function repeatStats(values: (number | null | undefined)[]) {
  const present = values.flatMap(value => value === null || value === undefined ? [] : [value])
  if (!present.length) return null
  return {
    count: present.length,
    mean: present.reduce((sum, value) => sum + value, 0) / present.length,
    min: Math.min(...present),
    max: Math.max(...present),
  }
}
