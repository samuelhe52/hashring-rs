export type JsonObject = Record<string, unknown>
export type RunStatus = 'success' | 'failed' | 'incomplete'

export interface RunListItem {
  id: string
  name: string
  source?: 'demo'
  status: RunStatus
  warnings: string[]
  config: JsonObject
  started_unix_ms?: number
  elapsed_seconds?: number
}

export interface Latency {
  mean?: number | null
  p50?: number | null
  p95?: number | null
  p99?: number | null
  p999?: number | null
  max?: number | null
  max_observed?: number | null
  over_range?: number | null
  samples?: number | null
}

export interface NodeLoad {
  owned_keys?: number | null
  ops_handled?: number | null
  cpu_percent?: number | null
}

export interface LoadBalance {
  per_node?: Record<string, NodeLoad>
  owned_keys_max_over_mean?: number | null
  owned_keys_cv?: number | null
  ops_handled_max_over_mean?: number | null
  ops_handled_cv?: number | null
}

export interface Measurement {
  target_rate?: number
  achieved_rate?: number
  seconds?: number
  sender_saturated?: boolean
  issued?: number
  late?: number
  retries?: number
  errors?: Record<string, number>
  ops?: Record<string, number>
  latency_us?: Record<string, Latency>
  load_balance?: LoadBalance
  operations?: number
  operations_per_second?: number
}

export interface Summary {
  success?: boolean
  error?: string | null
  elapsed_seconds?: number
  final_epoch?: number
  measurements?: Record<string, Measurement>
  observations?: JsonObject
}

export interface RunDetail {
  id: string
  name: string
  source?: 'demo'
  status: RunStatus
  warnings: string[]
  manifest: {
    schema_version?: number
    started_unix_ms?: number
    source_reproducible?: boolean
    config?: JsonObject
    build?: JsonObject
    system?: unknown
  } | null
  summary: Summary | null
}

export interface RunEvent {
  line: number
  event: string
  unix_ms: number
  fields: JsonObject
  raw: string
}

export interface EventLog {
  events: RunEvent[]
  issues: { line: number; message: string; raw: string }[]
  missing: boolean
}

export interface ReplayStart {
  log: EventLog
  time: number
  line: number | null
}

export interface ExperimentSession {
  id: string
  phase: 'starting' | 'running' | 'stopping' | 'succeeded' | 'failed' | 'stopped'
  startedAt: number
  endedAt: number | null
  command: string
  message: string
  hasResult: boolean
}

export interface ExperimentFeed {
  session: ExperimentSession | null
  log: EventLog
  stderr: string
  streamError: string
}
