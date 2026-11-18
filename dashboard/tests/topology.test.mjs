import test from 'node:test'
import assert from 'node:assert/strict'
import { HASH_SPACE, tokenValue, decodeTopology, assignmentAt, placementAt, topologySegments, spaceStats, ringBand } from '../client/src/topology.ts'

const snapshot = (positions, rf = 3, epoch = 1) => ({ epoch, desired_replication_factor: rf, members: [...new Set(positions.map(([, node]) => node))].map(node_id => ({ node_id })), assignments: positions.map(([token, node_id]) => ({ token: String(token), node_id })) })
const model = (...args) => { const parsed = decodeTopology(snapshot(...args)); assert.equal(parsed.error, ''); return parsed.model }
test('u64 保持精确，拒绝已舍入的 Number、负数和越界', () => {
  assert.equal(tokenValue('18446744073709551615'), HASH_SPACE - 1n)
  assert.throws(() => tokenValue(18446744073709551615))
  assert.throws(() => tokenValue('-1'))
  assert.throws(() => tokenValue(String(HASH_SPACE)))
})
test('区间右闭、跨零环绕；副本跳过同一物理节点，RF 包含 owner', () => {
  const m = model([[10, 'a'], [20, 'a'], [30, 'b'], [40, 'c']], 3)
  assert.equal(assignmentAt(m, 10n).node, 'a')
  assert.equal(assignmentAt(m, 31n).node, 'c')
  assert.equal(assignmentAt(m, 41n).node, 'a')
  assert.deepEqual(placementAt(m, 10n), ['a', 'b', 'c'])
  assert.equal(spaceStats(m).reduce((total, node) => total + node.length, 0n), HASH_SPACE)
})
test('单边界覆盖整环，同 token 的副本位置保留，RF 不虚构节点', () => {
  const m = model([[0, 'a'], [0, 'b']], 5)
  assert.equal(m.ranges.length, 1)
  assert.equal(m.ranges[0].length, HASH_SPACE)
  assert.deepEqual(m.ranges[0].replicas, ['a', 'b'])
  assert.equal(spaceStats(m).find(node => node.id === 'a').share, 100)
  assert.match(ringBand(0n, HASH_SPACE), /a149/)
})
test('联合边界精确比较 owner，不以 token 位置编号对齐；同 epoch 重复快照不伪造变化', () => {
  const a = model([[HASH_SPACE / 4n, 'a'], [HASH_SPACE * 3n / 4n, 'b']], 2)
  const b = model([[HASH_SPACE / 4n, 'a'], [HASH_SPACE / 2n, 'c'], [HASH_SPACE * 3n / 4n, 'b']], 2, 2)
  assert.equal(topologySegments(b, a).filter(part => part.changed).reduce((sum, part) => sum + part.length, 0n), HASH_SPACE / 4n)
  assert.equal(topologySegments(a, a).some(part => part.changed || part.replicasChanged), false)
  const largerRf = model([[HASH_SPACE / 4n, 'a'], [HASH_SPACE * 3n / 4n, 'b']], 1)
  assert.equal(topologySegments(a, largerRf).some(part => part.changed), false)
  assert.equal(topologySegments(a, largerRf).every(part => part.replicasChanged), true)
})
test('无效快照给出问题，不静默拼出部分环', () => {
  assert.equal(decodeTopology({}).model, null)
  const invalid = snapshot([[10, 'a']])
  invalid.assignments.push({ token: '20', node_id: 'unknown' })
  assert.match(decodeTopology(invalid).error, /未知节点/)
})
