import { formatNumber as fmt, number, object, text } from './data.ts'
import { eventName } from './events.ts'
import { visibleCount } from './history.ts'
import { decodeTopology, tokenValue } from './topology.ts'
import type { JsonObject, RunEvent, Summary } from './types'

export const scenes = {
  expansion: { title: '在线扩缩容', sample: 'verify-correctness-1794158675872' },
  fault: { title: '主节点故障', sample: 'verify-availability-1794158675872' },
} as const
export type Scene = keyof typeof scenes

export function sceneFor(config?: JsonObject): Scene | undefined {
  if (config?.mode === 'correctness' ||
      (config?.mode === 'scaling' && ['grow', 'shrink', 'round-trip', 'churn', 'replace'].includes(String(config.scaling_scenario)))) return 'expansion'
  if (config?.mode === 'availability' || (config?.mode === 'fault' && config.fault_scenario === 'owner')) return 'fault'
}

export const seconds = (value: unknown) => number(value)?.toFixed(2) ?? '未记录'
const isInjection = (event: RunEvent) => event.event === 'owner_killed' || event.event === 'fault_injected'
const isTransition = (event: RunEvent) => event.event === 'migration_started' || event.event === 'scaling_transition_started'
export function changeDirection(label: unknown) {
  if (label === 'scale_in' || label === 'shrink') return 'shrink'
  return label === 'replace' ? 'replace' : 'grow'
}

const milestones = {
  before: { label: '变更前', title: '扩容前的拓扑', note: '顺时针遇到的第一个虚拟位置决定主节点。', tone: 'neutral' },
  healthy: { label: '故障前', title: '故障前的节点归属', note: '环上颜色标出各区间当前的主节点。', tone: 'neutral' },
  grow: { label: '开始扩容', title: '启动在线扩容', note: '协调器开始变更成员。旧环继续提供数据归属，直到新拓扑发布。', tone: 'change' },
  shrink: { label: '开始缩容', title: '启动在线缩容', note: '旧主节点继续服务。新拓扑发布后，离开节点的区间并入顺时针的接管节点。', tone: 'change' },
  replace: { label: '开始替换', title: '启动节点替换', note: '新成员准备接管区间。当前归属在新拓扑发布前继续生效。', tone: 'change' },
  copy: { label: '复制期间', title: '复制期间仍有写入', note: '实验在快照复制期间记录了完成的改写和删除。', tone: 'change' },
  replay: { label: '增量追平', title: '回放变更日志', note: '迁移过程中发生的写入通过变更日志追平。', tone: 'change' },
  gate: { label: '限制写入', title: '对迁移区间设置写入栅栏', note: '协调器短暂限制这些区间的写入，准备校验并切换归属。', tone: 'change' },
  verify: { label: '发布前校验', title: '核对待切换区间', note: '在目标拓扑生效前检查待切换数据。', tone: 'change' },
  publish: { label: '发布拓扑', title: '新拓扑已发布', note: '实验记录了生效的拓扑版本。', tone: 'change' },
  migrated: { label: '迁移完成', title: '本次扩容迁移完成', note: '成员变更已完成。读回校验有单独的记录。', tone: 'change' },
  verified: { label: '迁移校验', title: '读回核对扩容结果', note: '实验核对了数据值及删除结果。', tone: 'ok' },
  snapshot: { label: '新环快照', title: '扩容后的完整快照', note: '环上高亮前后两份快照之间换了主节点的区间。', tone: 'ok' },
  killed: { label: '故障注入', title: '终止主节点进程', note: '从进程终止起计时，观察接管、读恢复与副本补齐。', tone: 'bad' },
  read: { label: '读取恢复', title: '读探针首次成功', note: '新拓扑下的读探针已成功，完整副本仍需后续补齐。', tone: 'ok' },
  restored: { label: '完整副本', title: '恢复到目标副本数', note: '替换节点加入后，受影响的数据恢复到目标副本数。', tone: 'ok' },
  round: { label: '本轮核验', title: '负载下的恢复结果', note: '本轮汇总记录了请求恢复与已确认状态的校验结果。', tone: 'ok' },
} as const
type Milestone = keyof typeof milestones
export interface CaseStage { event: RunEvent; kind: Milestone; change?: unknown }

function expansionPhase(event: RunEvent): Milestone | undefined {
  switch (event.event) {
    case 'migration_started':
    case 'scaling_transition_started': return changeDirection(event.fields.label)
    case 'migration_mutation_overlap_observed': return 'copy'
    case 'migration_complete':
    case 'scaling_transition_complete': return 'migrated'
    case 'migration_verification_complete':
    case 'scaling_cohorts_verified': return 'verified'
    case 'migration_phase_changed':
      switch (event.fields.phase) {
        case 'CopyingSnapshot': return 'copy'
        case 'ReplayingChangelog': return 'replay'
        case 'PausingWrites': return 'gate'
        case 'Verifying': return 'verify'
        case 'Published': return 'publish'
      }
  }
}

function faultPhase(event: RunEvent): Milestone | undefined {
  switch (event.event) {
    case 'owner_killed':
    case 'fault_injected': return 'killed'
    case 'failover_published':
    case 'fault_published': return 'publish'
    case 'failover_read_recovered': return 'read'
    case 'full_rf_restored':
    case 'fault_full_rf_restored': return 'restored'
    case 'fault_round_complete': return 'round'
  }
}

export function casePlayback(scene: Scene, events: RunEvent[]) {
  const expansion = scene === 'expansion'
  const isStart = expansion ? isTransition : isInjection
  const start = events.findIndex(event => isStart(event) && (expansion
    ? event.fields.control !== true
    : event.fields.scenario !== 'follower'))
  const stages: CaseStage[] = []
  const anchor = events[start]
  const label = anchor?.fields.label
  const round = anchor?.fields.round
  const initialRecords = start < 0 ? [] : events.slice(0, start)
  const initialSnapshot = initialRecords.filter(event => event.event === 'topology_published').reverse()
    .find(event => decodeTopology(event.fields).model)

  if (anchor) {
    const next = expansion ? -1 : events.findIndex((event, index) => index > start && isInjection(event))
    const scope = events.slice(start, next < 0 ? undefined : next)
    if (expansion) {
      const before = initialRecords.filter(event => event.event === 'initial_get_verify_complete').at(-1) ?? initialSnapshot
      if (before) stages.push({ event: before, kind: 'before', change: label })
    } else if (initialSnapshot) {
      stages.push({ event: initialSnapshot, kind: 'healthy' })
    }
    let change: RunEvent | undefined
    let recordedEpoch = epochText(initialSnapshot?.fields.epoch)
    for (const event of scope) {
      if (expansion && isTransition(event)) change = event.fields.control !== true ? event : undefined
      const related = expansion ? change && event.fields.label === change.fields.label : event.fields.round === round
      const kind = related ? (expansion ? expansionPhase(event) : faultPhase(event)) : undefined
      if (kind) stages.push({ event, kind, change: change?.fields.label })
      else if (change && event.event === 'topology_published') {
        const snapshot = decodeTopology(event.fields).model
        const targetEpoch = epochText(change.fields.target_epoch)
        if (snapshot && snapshot.epoch !== recordedEpoch && (!targetEpoch || snapshot.epoch === targetEpoch)) {
          recordedEpoch = snapshot.epoch
          stages.push({ event, kind: 'snapshot', change: change.fields.label })
        }
      }
    }
  }
  const last = stages.at(-1)?.event
  const records = last ? events.slice(0, visibleCount(events, last.unix_ms, last.line)) : events
  return { anchor, label, round, stages, records, initialSnapshot }
}
type CasePlayback = ReturnType<typeof casePlayback>

function epochText(value: unknown) {
  if (value == null) return undefined
  try { return tokenValue(value).toString() } catch { return undefined }
}

export function latestPublishedEpoch(events: RunEvent[]) {
  for (let index = events.length - 1; index >= 0; index--) {
    const { event, fields } = events[index]!
    let epoch: unknown
    if (['topology_published', 'failover_published', 'fault_published', 'full_rf_restored', 'fault_full_rf_restored'].includes(event)) epoch = fields.epoch
    if (event === 'migration_phase_changed' && ['Published', 'CleaningUp', 'Complete'].includes(String(fields.phase))) epoch = fields.epoch
    if (event === 'scaling_transition_complete') epoch = fields.target_epoch
    const recorded = epochText(epoch)
    if (recorded !== undefined) return recorded
  }
}

function roundSummary(summary: Summary | null | undefined, replay: CasePlayback) {
  const observations = object(summary?.observations)
  for (const list of [observations.availability_rounds, observations.fault_rounds]) {
    if (Array.isArray(list)) {
      const match = list.map(object).find(item => item.round === replay.round)
      if (match) return match
    }
  }
  return replay.round === 1 || replay.round === undefined ? observations : {}
}

function faultResult(summary: Summary | null | undefined, replay: CasePlayback) {
  const round = roundSummary(summary, replay)
  const scope = replay.records.filter(event => event.fields.round === replay.round)
  const read = replay.stages.find(stage => stage.kind === 'read')?.event.fields
  const restored = replay.stages.find(stage => stage.kind === 'restored')?.event.fields
  const survival = object(scope.find(event => event.event === 'acknowledged_key_survival_checked')?.fields ?? round.acknowledged_key_survival)
  return {
    read: number(read?.read_recovery_seconds) ?? number(round.read_recovery_seconds),
    fullRf: number(restored?.failure_to_full_rf_seconds) ?? number(round.failure_to_full_rf_seconds),
    preserved: number(survival.preserved), sampled: number(survival.sampled),
    droppedStates: number(round.dropped_acknowledged_states),
  }
}

function stageSeconds(stage: CaseStage, replay: CasePlayback, summary?: Summary | null) {
  const { event, kind } = stage
  const anchor = replay.anchor ?? event
  const elapsed = (event.unix_ms - anchor.unix_ms) / 1000
  if (isInjection(anchor)) {
    if (kind === 'publish') return number(event.fields.seconds) ?? elapsed
    if (kind === 'read') return number(event.fields.read_recovery_seconds) ?? elapsed
    if (kind === 'restored') return faultResult(summary, replay).fullRf ?? elapsed
  }
  return elapsed
}

export function headline(scene: Scene, replay: CasePlayback, summary?: Summary | null) {
  if (scene === 'fault') {
    const result = faultResult(summary, replay)
    return [
      { label: '首次成功读', value: seconds(result.read), unit: 's', note: '从终止主节点起算', tone: 'ok' },
      { label: '完整副本恢复', value: seconds(result.fullRf), unit: 's', note: '从终止主节点起算', tone: 'ok' },
      result.droppedStates !== null
        ? { label: '丢失的已确认状态', value: fmt(result.droppedStates), unit: '条', note: '本轮负载核验', tone: 'neutral' }
        : { label: '抽样键保留', value: result.preserved === null || result.sampled === null ? '未记录' : fmt(result.preserved) + ' / ' + fmt(result.sampled), unit: '', note: '已确认写入抽样', tone: 'neutral' },
    ]
  }
  const verification = replay.stages.find(stage => stage.kind === 'verified')?.event.fields
  const transition = object(object(summary?.observations)[String(replay.label) + '_transition'])
  const migration = object(transition.migration)
  const moved = number(migration.keys_moved) ?? number(replay.anchor?.fields.keys_moved)
  const checked = { label: '受检键', value: number(verification?.keys) === null ? '未记录' : fmt(verification?.keys), unit: '键', note: '变更后的读回校验', tone: 'ok' }
  if (moved !== null) {
    const total = number(migration.keys)
    return [
      { label: '换主键数', value: fmt(moved), unit: '键', note: total !== null && total > 0 ? '共 ' + fmt(total) + ' 键 · ' + fmt(moved / total * 100, 2) + '%' : '扩容前后逐键比较', tone: 'change' },
      checked,
      { label: '校验不符', value: number(verification?.violations) === null ? '未记录' : fmt(verification?.violations), unit: '项', note: '变更键组核验', tone: 'ok' },
    ]
  }
  return [
    checked,
    { label: '改写检查', value: number(verification?.rewritten_moving_keys) === null ? '未记录' : fmt(verification?.rewritten_moving_keys), unit: '键', note: '受检键中的改写子集', tone: 'neutral' },
    { label: '删除检查', value: number(verification?.deleted_moving_keys) === null ? '未记录' : fmt(verification?.deleted_moving_keys), unit: '键', note: '受检键中的删除子集', tone: 'neutral' },
  ]
}

export function stageDetails(stage: CaseStage, replay: CasePlayback, summary?: Summary | null) {
  const { kind, event } = stage
  const f = event.fields
  const copyObserved = kind === 'copy' && event.event === 'migration_mutation_overlap_observed'
  const direction = changeDirection(stage.change)
  const changeName = { grow: '扩容', shrink: '缩容', replace: '替换' }[direction]
  const base = milestones[kind]
  const label = kind === 'migrated' ? changeName + '完成' : kind === 'verified' ? changeName + '校验' : base.label
  const labels = kind === 'copy' && !copyObserved
    ? { ...milestones.copy, title: '复制快照', note: '实验观测到迁移进入快照复制阶段。' }
    : { ...base, label, title: base.title.replace('扩容', changeName) }
  let facts: [string, unknown, string?, RunEvent?][] = []
  switch (kind) {
    case 'healthy':
    case 'before': facts = [['初始读校验', f.keys, '键'], ['起始 epoch', replay.initialSnapshot?.fields.epoch]]; break
    case 'shrink':
    case 'replace':
    case 'grow': facts = [['起始 epoch', f.base_epoch ?? (event === replay.anchor ? replay.initialSnapshot?.fields.epoch : undefined)], ['目标 epoch', f.target_epoch], ['换主键数', f.keys_moved, '键']]; break
    case 'copy': facts = [['观测阶段', f.observed_phase ?? f.phase], ['复制期间完成的变更', f.completed_mutations_while_active, '次']]; break
    case 'replay':
    case 'gate':
    case 'verify': facts = [['目标 epoch', f.epoch]]; break
    case 'migrated': {
      const migration = object(object(object(summary?.observations)[String(stage.change) + '_transition']).migration)
      const reasons = Object.values(object(f.retries_by_reason)).map(number)
      const retries = reasons.length && reasons.every(count => count !== null)
        ? reasons.reduce<number>((sum, count) => sum + (count ?? 0), 0) : null
      facts = [['迁移执行用时', number(f.seconds)?.toFixed(2), 's'], ['改写', f.writes, '键'], ['删除', f.deletes, '键'],
        ['快照搬运', migration.snapshot_records, '条'], ['变更期间重试', retries, '次'], ['目标 epoch', f.target_epoch]]
      break
    }
    case 'verified': facts = [['受检', f.keys, '键'], ['改写检查', f.rewritten_moving_keys, '键'], ['恢复检查', f.restored_moving_keys, '键'], ['删除检查', f.deleted_moving_keys, '键'], ['不符', f.violations, '项']]; break
    case 'snapshot':
    case 'publish': facts = [['已发布 epoch', f.epoch]]; break
    case 'killed': facts = [['故障节点', f.node_id ?? f.victim], ['退出信号', f.exit]]; break
    case 'read': facts = [['未成功的读探针', f.unavailable_probe_count, '次'], ['观测最长连续不可用', f.observed_unavailable_max_streak_ms, 'ms']]; break
    case 'restored': {
      const atStage = replay.records.slice(0, visibleCount(replay.records, event.unix_ms, event.line))
      const survivalEvent = atStage.filter(item => item.event === 'acknowledged_key_survival_checked' && item.fields.round === replay.round).at(-1)
      const writeEvent = atStage.filter(item => item.event === 'write_after_failover' && item.fields.round === replay.round).at(-1)
      const survival = survivalEvent?.fields
      const write = writeEvent?.fields
      facts = [['已发布 epoch', f.epoch],
        ['抽样键保留', number(survival?.preserved) !== null && number(survival?.sampled) !== null ? fmt(survival?.preserved) + ' / ' + fmt(survival?.sampled) : null, undefined, survivalEvent],
        ['写探针自身耗时', number(write?.seconds)?.toFixed(2), 's', writeEvent],
        ['替换至满 RF', number(f.replacement_to_full_rf_seconds)?.toFixed(2), 's']]
      break
    }
    case 'round': facts = [['从故障到首次成功读', number(f.read_recovery_seconds)?.toFixed(2), 's'],
      ['从故障到首次成功写', number(f.write_recovery_seconds)?.toFixed(2), 's'], ['丢失的已确认状态', f.dropped_acknowledged_states, '条']]; break
  }
  return { ...labels, elapsed: stageSeconds(stage, replay, summary), eventName: eventName(event.event),
    facts: facts.filter(([, value]) => value != null)
      .map(([label, value, unit, source]) => ({ label, value: (typeof value === 'number' ? fmt(value, 3) : text(value)) + (unit ? ' ' + unit : ''), source })) }
}
