import { colors, number, object } from './data.ts'

export const HASH_SPACE = 1n << 64n
export const nodePalette = [...colors, '#7d8850', '#99687d', '#4b848f', '#806844', '#5d7393', '#597e60']
export interface Assignment { token: bigint; node: string }
export interface RingRange { start: bigint; end: bigint; length: bigint; owner: string; replicas: string[] }
export interface RingModel { epoch: string; rf: number; members: { id: string; endpoint: string }[]; assignments: Assignment[]; ranges: RingRange[] }

export function tokenValue(value: unknown): bigint {
  if (!(typeof value === 'string' && /^\d+$/.test(value)) && !(typeof value === 'number' && Number.isSafeInteger(value) && value >= 0)) throw new Error('token / epoch 必须是精确的 u64 十进制值，不能使用已舍入的 Number')
  const token = BigInt(value as string | number)
  if (token < 0n || token >= HASH_SPACE) throw new Error('token / epoch 超出 u64 范围')
  return token
}

export function assignmentAt(model: RingModel, token: bigint): Assignment {
  let low = 0
  let high = model.assignments.length
  while (low < high) { const mid = (low + high) >>> 1; if (model.assignments[mid]!.token < token) low = mid + 1; else high = mid }
  return model.assignments[low % model.assignments.length]!
}

// 与 topology.rs 一致：从第一个 >= token 的位置顺时针收集不同物理节点。
export function placementAt(model: RingModel, token: bigint): string[] {
  const owner = assignmentAt(model, token)
  const index = model.assignments.indexOf(owner)
  const target = Math.min(model.rf, model.members.length)
  const nodes = new Set<string>()
  for (let offset = 0; offset < model.assignments.length && nodes.size < target; offset++) nodes.add(model.assignments[(index + offset) % model.assignments.length]!.node)
  return [...nodes]
}

export function decodeTopology(value: unknown): { model: RingModel | null; error: string } {
  try {
    const fields = object(value)
    if (!Array.isArray(fields.members) || !fields.members.length) throw new Error('缺少有效的 members')
    const members = fields.members.map(item => { const member = object(item); if (typeof member.node_id !== 'string' || !member.node_id) throw new Error('成员缺少 node_id'); return { id: member.node_id, endpoint: typeof member.endpoint === 'string' ? member.endpoint : '未提供' } })
    if (new Set(members.map(member => member.id)).size !== members.length) throw new Error('members 存在重复 node_id')
    const rf = number(fields.desired_replication_factor)
    if (rf === null || !Number.isSafeInteger(rf) || rf < 1) throw new Error('缺少有效的期望 RF')
    if (!Array.isArray(fields.assignments) || !fields.assignments.length) throw new Error('缺少有效的 assignments')
    const assignments = fields.assignments.map(item => { const assignment = object(item); if (typeof assignment.node_id !== 'string' || !members.some(member => member.id === assignment.node_id)) throw new Error('assignment 引用了未知节点'); return { token: tokenValue(assignment.token), node: assignment.node_id } }).sort((a, b) => a.token < b.token ? -1 : a.token > b.token ? 1 : 0)
    const model: RingModel = { epoch: tokenValue(fields.epoch).toString(), rf, members, assignments, ranges: [] }
    // 相同 token 只形成一个边界，但副本遍历仍保留全部位置和原始先后顺序。
    const ends = [...new Set(assignments.map(assignment => assignment.token))]
    model.ranges = ends.map((end, index) => { const start = ends[(index - 1 + ends.length) % ends.length]!; const replicas = placementAt(model, end); return { start, end, length: ends.length === 1 ? HASH_SPACE : (end - start + HASH_SPACE) % HASH_SPACE, owner: replicas[0]!, replicas } })
    return { model, error: '' }
  } catch (error) { return { model: null, error: error instanceof Error ? error.message : String(error) } }
}

export function topologySegments(current: RingModel, previous: RingModel | null) {
  const ends = [...new Set([...current.assignments, ...(previous?.assignments ?? [])].map(item => item.token))].sort((a, b) => a < b ? -1 : a > b ? 1 : 0)
  return ends.map((end, index) => {
    const start = ends[(index - 1 + ends.length) % ends.length]!
    const replicas = placementAt(current, end)
    const oldReplicas = previous ? placementAt(previous, end) : replicas
    return { start, end, length: ends.length === 1 ? HASH_SPACE : (end - start + HASH_SPACE) % HASH_SPACE, owner: replicas[0]!, previousOwner: oldReplicas[0]!, replicas, oldReplicas,
      rangeEnd: assignmentAt(current, end).token, changed: replicas[0] !== oldReplicas[0], replicasChanged: JSON.stringify(replicas) !== JSON.stringify(oldReplicas) }
  })
}

export function spaceShare(length: bigint): number { return Number(length * 1_000_000n / HASH_SPACE) / 10_000 }
export function spaceStats(model: RingModel) {
  return model.members.map(member => {
    const length = model.ranges.filter(range => range.owner === member.id).reduce((sum, range) => sum + range.length, 0n)
    return { ...member, length, share: spaceShare(length), tokens: model.assignments.filter(assignment => assignment.node === member.id).length }
  })
}

// 只在绘图时转为角度；归属、区间与比较始终使用 BigInt。
export function ringAngle(token: bigint): number { return Number(token * 1_000_000_000_000n / HASH_SPACE) / 1_000_000_000_000 * 360 }
export function ringPoint(angle: number, radius: number, center = 180): [number, number] { const radians = angle * Math.PI / 180; return [center + radius * Math.sin(radians), center - radius * Math.cos(radians)] }
export function ringBand(start: bigint, length: bigint, inner = 124, outer = 149, center = 180): string {
  if (length === HASH_SPACE) return `M${center} ${center - outer}a${outer} ${outer} 0 1 1 0 ${outer * 2}a${outer} ${outer} 0 1 1 0 ${-outer * 2}M${center} ${center - inner}a${inner} ${inner} 0 1 0 0 ${inner * 2}a${inner} ${inner} 0 1 0 0 ${-inner * 2}Z`
  const angle = ringAngle(start)
  const span = ringAngle(length)
  const a = ringPoint(angle, outer, center), b = ringPoint(angle + span, outer, center), c = ringPoint(angle + span, inner, center), d = ringPoint(angle, inner, center)
  const large = span > 180 ? 1 : 0
  return `M${a[0]} ${a[1]}A${outer} ${outer} 0 ${large} 1 ${b[0]} ${b[1]}L${c[0]} ${c[1]}A${inner} ${inner} 0 ${large} 0 ${d[0]} ${d[1]}Z`
}

export function nodeColor(node: string, palette: string[]): string {
  const numbered = /^node-(\d+)$/.exec(node)
  if (numbered && Number.isSafeInteger(Number(numbered[1])) && Number(numbered[1]) > 0) return palette[(Number(numbered[1]) - 1) % palette.length]!
  let hash = 0
  for (const character of node) hash = (hash * 31 + character.charCodeAt(0)) >>> 0
  return palette[hash % palette.length]!
}
