import type { RunEvent } from './types'
import { formatNumber, formatTime, text } from './data.ts'

export const categories = {
  lifecycle: '实验进程', metrics: '采样指标', topology: '拓扑', migration: '迁移与扩缩容', fault: '故障与恢复', other: '其他',
}
export type EventCategory = keyof typeof categories

const names: Record<string, string> = {
  experiment_started: '实验启动', cluster_ready: '集群就绪', experiment_finished: '实验结束',
  initial_put_complete: '初始写入完成', initial_get_verify_complete: '初始读校验完成', verification_complete: '最终核验完成',
  metrics_sampled: '指标采样', node_load_sampled: '节点请求采样', node_pressure_sampled: '节点压力采样',
  client_put_latency_sampled: '客户端空闲延迟采样', owner_rpc_put_latency_sampled: 'Owner RPC 空闲延迟采样',
  topology_published: '拓扑快照已记录', ack_policy_activated: 'ACK 策略已生效',
  migration_started: '迁移开始', migration_execution_finished: '迁移执行结束', migration_complete: '迁移完成',
  migration_verification_complete: '迁移核验完成', migration_writer_failed: '迁移期间写入失败',
  migration_mutation_overlap_observed: '记录迁移与变更重叠', migration_phase_changed: '迁移阶段改变',
  removed_node_exited: '移除节点已退出', direct_merge_cutover_observed: '记录直接合并切换',
  owner_killed: 'Owner 已终止', failover_sample_selected: '故障核验样本已选定', failover_published: '故障转移已发布',
  failover_read_recovered: '故障后读取恢复', write_after_failover: '故障后写入结果', full_rf_restored: '完整副本已恢复',
  acknowledged_key_survival_checked: '已确认写入存留核验', availability_round_balanced: '本轮负载分布已记录',
  availability_rounds_complete: '连续故障实验结束',
  fault_cohort_selected: '故障负载键组已选定', fault_round_started: '故障轮次开始',
  fault_load_started: '故障负载已启动', fault_pre_failure_replica_progress: '故障前副本进度',
  fault_injected: '故障已注入', fault_published: '故障拓扑已发布', fault_full_rf_restored: '故障后完整副本已恢复',
  fault_post_failure_status_failed: '故障后状态读取失败', fault_round_complete: '故障轮次结束', fault_run_complete: '故障实验结束',
  scaling_warmup_started: '扩缩容负载预热', scaling_transition_started: '扩缩容变更开始',
  scaling_transition_complete: '扩缩容变更结束', scaling_complete: '扩缩容实验结束',
  scaling_cohorts_verified: '扩缩容键组核验完成', scaling_control_window_started: '扩缩容基线计时开始',
  scaling_after_publish_probe_started: '发布后探测启动', scaling_after_publish_probe_skipped: '发布后探测跳过',
  scaling_after_publish_probe_stuck: '发布后探测阻塞',
}

export function eventCategory(name: string): EventCategory {
  if (/^(fault_|failover_|availability_|owner_killed|full_rf_restored|write_after_failover|acknowledged_key_survival)/.test(name)) return 'fault'
  if (/^(migration_|scaling_|removed_node_|direct_merge_)/.test(name)) return 'migration'
  if (name === 'topology_published' || name === 'ack_policy_activated') return 'topology'
  if (name.endsWith('_sampled')) return 'metrics'
  if (Object.hasOwn(names, name)) return 'lifecycle'
  return 'other'
}

export function eventName(name: string): string { return Object.hasOwn(names, name) ? names[name] : name }
export function isKeyEvent(event: RunEvent): boolean { return eventCategory(event.event) !== 'metrics' }
export function eventStage(event: RunEvent): string {
  const value = event.fields.stage ?? event.fields.phase
  return typeof value === 'string' ? value : ''
}

export function eventSummary(event: RunEvent): string {
  const f = event.fields
  const parts: string[] = []
  const stage = eventStage(event)
  if (stage) parts.push(stage)
  if (typeof f.warmup === 'boolean') parts.push(f.warmup ? '预热' : '正式采样')
  if (f.epoch !== undefined && f.epoch !== null) parts.push(`epoch ${text(f.epoch)}`)
  if (event.event === 'metrics_sampled') {
    if (f.target_rate !== undefined) parts.push(`目标 ${formatNumber(f.target_rate)} ops/s`)
    if (f.issued !== undefined) parts.push(`发出 ${formatNumber(f.issued)}`)
  }
  if (event.event === 'topology_published' && Array.isArray(f.members)) parts.push(`${f.members.length} 个节点`)
  if (typeof f.success === 'boolean') parts.push(f.success ? '成功' : '失败')
  if (f.keys !== undefined) parts.push(`${text(f.keys)} 键`)
  if (typeof f.error === 'string') parts.push(f.error)
  return parts.join(' · ')
}

export function absoluteEventTime(unixMs: number): string {
  return `${formatTime(unixMs)}.${String(unixMs % 1000).padStart(3, '0')}`
}

export function relativeEventTime(unixMs: number, origin: number | null): string {
  if (origin === null) return '—'
  const seconds = (unixMs - origin) / 1000
  return `${seconds >= 0 ? '+' : '−'}${Math.abs(seconds).toFixed(3)} s`
}
