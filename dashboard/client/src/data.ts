import type { JsonObject, Latency, Measurement, RunStatus, Summary } from './types'

export const operations = ['get', 'put', 'delete'] as const
export const statusLabels: Record<RunStatus, string> = { success: '成功', failed: '失败', incomplete: '未完成' }
export const modeLabels: Record<string, string> = {
  latency: '负载与延迟',
  performance: '性能测试',
  correctness: '正确性与迁移',
  availability: '可用性与故障转移',
  scaling: '负载下的扩缩容',
  fault: '负载下的故障恢复',
}
export const colors = ['#b4532a', '#2d7774', '#a07a35', '#6a6282', '#5576a0']

export function object(value: unknown): JsonObject {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as JsonObject : {}
}

export function number(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

export function latencyStatistic(value: unknown, metric: string): number | null {
  const stats = object(value)
  if (stats.samples === 0 && metric !== 'samples' && metric !== 'over_range') return null
  return number(stats[metric])
}

export function formatNumber(value: unknown, decimals = 0): string {
  const n = number(value)
  return n === null ? '—' : n.toLocaleString('en-US', { maximumFractionDigits: decimals })
}

export function text(value: unknown): string {
  if (value === null || value === undefined) return '未提供'
  if (value === true) return '是'
  if (value === false) return '否'
  return typeof value === 'object' ? JSON.stringify(value) : String(value)
}

export function formatTime(value: unknown): string {
  const n = number(value)
  if (n === null || !Number.isFinite(new Date(n).getTime())) return '时间未提供'
  return new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  }).format(n)
}

export function shortName(id: string): string {
  return id.replace(/-\d{8}-\d{6}$/, '')
}

// 只整理 summary 已有的阶段；不从窗口分位数聚合成绩。
export function stages(summary: Summary | null): [string, Measurement][] {
  return Object.entries(object(summary?.measurements))
    .filter(([, value]) => number(object(value).target_rate) !== null)
    .map(([key, value]) => [key, value as Measurement] as [string, Measurement])
    .sort(([a], [b]) => a.localeCompare(b, 'en', { numeric: true }))
}

export function errorsText(errors: unknown): string {
  const entries = Object.entries(object(errors))
  if (!entries.length) return '未提供'
  const nonzero = entries.filter(([, value]) => value !== 0)
  return nonzero.length ? nonzero.map(([name, value]) => `${name}: ${text(value)}`).join(' / ') : '全为 0'
}

export function latencyRows(value: unknown): [string, Latency][] {
  const entries = object(value)
  return Object.entries(entries)
    .filter(([, stats]) => Object.keys(object(stats)).length > 0)
    .map(([name, stats]) => [name, stats as Latency])
}

export function throughputRows(summary: Summary | null): [string, Measurement][] {
  const observations = object(summary?.observations)
  const measurements = object(summary?.measurements)
  const values = { ...observations, ...measurements }
  return Object.entries(values)
    .filter(([, value]) => number(object(value).operations_per_second) !== null)
    .map(([key, value]) => [key, value as Measurement])
}

export const fieldNames: Record<string, string> = {
  mode: '实验模式', node_count: '数据节点数', key_count: '键数', value_bytes: '值大小（B）',
  desired_replication_factor: '期望副本数 RF', write_ack_policy: '写确认策略', virtual_nodes: '每节点虚拟位置数',
  concurrency: '批量步骤并发数', target_rates: '目标速率（ops/s）', warmup_seconds: '阶段预热（s）',
  duration_seconds: '阶段计时（s）', preload_keys: '预加载键数', key_distribution: '键访问分布',
  mix: '操作配比 GET:PUT:DELETE', max_in_flight: '在途请求上限', hash_seed: '哈希种子',
  initial_put: '初始写入', initial_get_verify: '初始读校验',
  initial_full_rf_seconds: '初始完整 RF（s）', failover_seconds: '故障转移（s）',
  read_recovery_seconds: '读取恢复（s）', replacement_to_full_rf_seconds: '替换至完整 RF（s）',
  failure_to_full_rf_seconds: '故障至完整 RF（s）', latency_verified_keys: '负载后核验键数',
  latency_delete_pool_keys: '删除池键数', latency_key_distribution: '负载键分布',
  latency_key_space: '负载键空间', latency_mix: '负载操作配比',
}

export async function request<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, { signal })
  if (!response.ok) {
    const body = await response.json().catch(() => ({}))
    throw new Error(body.error || `读取失败（HTTP ${response.status}）`)
  }
  return response.json()
}

export async function requestText(url: string, signal?: AbortSignal): Promise<string> {
  const response = await fetch(url, { signal })
  if (!response.ok) {
    const body = await response.json().catch(() => ({}))
    throw new Error(body.error || `读取失败（HTTP ${response.status}）`)
  }
  return response.text()
}
