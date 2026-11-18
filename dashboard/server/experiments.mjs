import { execFile, spawn } from 'node:child_process'
import { constants } from 'node:fs'
import { access, mkdir, open, readFile, readdir, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { StringDecoder } from 'node:string_decoder'
import { promisify } from 'node:util'
import { parseEventLog, parseRecordedJson } from './events.mjs'

const execute = promisify(execFile)
const modes = ['correctness', 'performance', 'availability', 'latency', 'scaling', 'fault']
const policies = ['owner-only', 'first-successor', 'all-replicas']
const scalingReserves = { 'round-trip': 1, replace: 1, churn: 3 }
const terminal = new Set(['succeeded', 'failed', 'stopped'])
const emptyLog = () => ({ events: [], issues: [], missing: false })
const quote = value => "'" + String(value).replaceAll("'", "'\"'\"'") + "'"
const problem = (status, message) => Object.assign(new Error(message), { status })

function positive(value, label, min = 1) {
  if (!Number.isSafeInteger(value) || value < min) throw problem(400, `${label}须为不小于 ${min} 的整数`)
  return String(value)
}

function u64(value, label, min = 0n) {
  const digits = String(value)
  if (!/^\d{1,20}$/.test(digits) || BigInt(digits) < min || BigInt(digits) > 18446744073709551615n) {
    throw problem(400, `${label}超出允许的整数范围`)
  }
  return BigInt(digits).toString()
}

export function experimentPlan(input, { resultsRoot, repoRoot, binary }) {
  const id = input?.name
  if (typeof id !== 'string' || !/^[a-z0-9][a-z0-9_-]{0,79}$/i.test(id)) throw problem(400, '结果目录名须为 1–80 个字母、数字、短横线或下划线，并以字母或数字开头')
  const output = path.join(resultsRoot, id)
  const args = ['experiment', '--output', output]
  let yaml
  let configFile
  if (input.kind === 'yaml') {
    yaml = input.yaml
    if (typeof yaml !== 'string' || !yaml.trim() || yaml.includes('\0')) throw problem(400, '请提供有效的 YAML 文本')
    configFile = path.join(resultsRoot, '.dashboard-config', id + '.yaml')
    args.push('--config', configFile)
  } else {
    const { mode, policy } = input
    if (!modes.includes(mode)) throw problem(400, '实验模式无效')
    if (!policies.includes(policy)) throw problem(400, 'ACK 策略无效')
    const nodes = positive(input.nodes, '节点数', 2)
    const keys = positive(input.keys, '键数')
    const rf = positive(input.rf, 'RF')
    const availabilityRounds = mode === 'availability' ? positive(input.availabilityRounds ?? 1, '连续故障轮数') : '1'
    const scalingScenario = input.scalingScenario ?? 'round-trip'
    if (mode === 'scaling' && (typeof scalingScenario !== 'string' || !Object.hasOwn(scalingReserves, scalingScenario))) {
      throw problem(400, '扩缩容场景无效')
    }
    const minimumNodes = { availability: 4, fault: 5 }[mode] ?? 2
    const reserves = { correctness: 1, availability: Number(availabilityRounds), scaling: scalingReserves[scalingScenario], fault: 1 }[mode] ?? 0
    if (input.nodes < minimumNodes || input.nodes - reserves < input.rf) throw problem(400, '起始节点数不足以满足当前模式与 RF')
    if (['availability', 'fault'].includes(mode) && (input.rf !== 3 || policy === 'all-replicas')) throw problem(400, '故障模式要求 RF 为 3，ACK 请选择 OwnerOnly 或 FirstSuccessor')
    args.push('--mode', mode, '--nodes', nodes, '--keys', keys, '--desired-replication-factor', rf,
      '--concurrency', positive(input.concurrency, '并发度'), '--value-bytes', positive(input.valueBytes, '值大小'),
      '--seed', u64(input.seed, '哈希种子'), '--virtual-nodes', positive(input.virtualNodes, '每节点虚拟位置'),
      '--policy', policy)
    if (input.workerThreads !== undefined && input.workerThreads !== '') {
      args.push('--worker-threads', positive(input.workerThreads, '工作线程数'))
    }
    if (mode === 'availability') args.push('--availability-rounds', availabilityRounds)
    if (mode === 'scaling' && input.scalingScenario !== undefined) {
      args.push('--scaling-scenario', scalingScenario, '--pre-publish-delay-ms', '0')
    }
    if (mode === 'latency') {
      const rates = String(input.rates ?? '').trim().split(/[\s,，]+/).filter(Boolean)
      if (!rates.length) throw problem(400, '至少填写一个目标速率')
      if (typeof input.mix !== 'string' || !/^\d+:\d+:\d+$/.test(input.mix) || !input.mix.split(':').some(part => Number(part) > 0)) throw problem(400, '操作配比应为 GET:PUT:DELETE，例如 90:9:1')
      if (typeof input.distribution !== 'string' || !/^(uniform|zipf:[\d.]+|hotspot:[\d.]+:[\d.]+)$/.test(input.distribution)) throw problem(400, '键分布应为 uniform、zipf:0.99 或 hotspot:0.2:0.8 等格式')
      args.push('--preload-keys', keys, '--target-rates', rates.map(rate => u64(rate, '目标速率', 1n)).join(','),
        '--warmup-seconds', positive(input.warmup, '预热时长', 0), '--duration-seconds', positive(input.duration, '正式时长'),
        '--mix', input.mix, '--key-distribution', input.distribution,
        '--max-in-flight', positive(input.maxInFlight, '在途请求上限'))
      if (input.dedupMiB !== undefined) {
        const budgetMiB = Number(positive(input.dedupMiB, '每节点回执预算'))
        args.push('--max-dedup-bytes', positive(budgetMiB * 1024 * 1024, '每节点回执预算（字节）'))
      }
    }
  }
  const command = [binary, ...args].map(quote).join(' ')
  let script = '#!/usr/bin/env bash\nset -euo pipefail\ncd ' + quote(repoRoot) + '\n'
  script += 'output=${1:-' + quote(output) + '}\n'
  const base = quote(binary) + ' experiment --output "$output"'
  if (yaml !== undefined) {
    let marker = 'HASHRING_YAML'
    const lines = yaml.split(/\r?\n/)
    while (lines.includes(marker)) marker += '_'
    script += 'config=$(mktemp)\ntrap \'rm -f -- "$config"\' EXIT\ncat > "$config" <<' + quote(marker) + '\n'
    script += yaml.replace(/\r\n/g, '\n').replace(/\n?$/, '\n') + marker + '\n' + base + ' --config "$config"\n'
  } else {
    script += 'exec ' + base + ' ' + args.slice(3).map(quote).join(' ') + '\n'
  }
  return { id, output, args, command, yaml, configFile, script }
}

function followEvents(filename) {
  const decoder = new StringDecoder('utf8')
  let position = 0
  let lines = 0
  let pending = ''
  return async function readNew(final = false) {
    const file = await open(filename, 'r').catch(error => {
      if (error.code !== 'ENOENT') throw error
      return null
    })
    if (!file) return emptyLog()
    try {
      const size = (await file.stat()).size
      const buffer = Buffer.alloc(Math.min(Math.max(size - position, 1), 65536))
      while (position < size) {
        const { bytesRead } = await file.read(buffer, 0, Math.min(buffer.length, size - position), position)
        if (!bytesRead) break
        position += bytesRead
        pending += decoder.write(buffer.subarray(0, bytesRead))
      }
    } finally {
      await file.close()
    }
    if (final) pending += decoder.end()
    const end = final ? pending.length : pending.lastIndexOf('\n') + 1
    const chunk = pending.slice(0, end)
    pending = pending.slice(end)
    const parsed = parseEventLog(chunk, lines)
    lines += chunk.split('\n').length - 1
    return parsed
  }
}

async function coordinatorAddress(run) {
  if (run.coordinator) return run.coordinator
  // 旧实验二进制默认不写 INFO 日志，Linux/WSL 可从本次进程的子进程读取监听参数。
  if (process.platform === 'linux' && run.child.pid) {
    const tasks = `/proc/${run.child.pid}/task`
    const threads = await readdir(tasks).catch(() => [])
    const lists = await Promise.all(threads.map(tid => readFile(`${tasks}/${tid}/children`, 'utf8').catch(() => '')))
    const children = new Set(lists.join(' ').match(/\d+/g) ?? [])
    for (const pid of children) {
      const argv = (await readFile(`/proc/${pid}/cmdline`, 'utf8').catch(() => '')).split('\0')
      if (argv[1] !== 'coordinator' || argv[argv.indexOf('--state') + 1] !== path.join(run.output, 'coordinator.redb')) continue
      const address = argv[argv.indexOf('--listen') + 1]
      if (/^127\.0\.0\.1:\d{1,5}$/.test(address ?? '')) return address
    }
  }
  const log = await open(path.join(run.output, 'process-logs/coordinator.stderr.log'), 'r').catch(() => null)
  if (!log) return null
  try {
    const buffer = Buffer.alloc(8192)
    const { bytesRead } = await log.read(buffer)
    return /coordinator ready listen=(127\.0\.0\.1:\d{1,5})\b/.exec(buffer.toString('utf8', 0, bytesRead))?.[1]
  } finally { await log.close() }
}

export function createExperiments(options) {
  const { resultsRoot, repoRoot, binary } = options
  const clients = new Set()
  let current = null
  let starting = false
  let closing = false
  const active = () => current && !terminal.has(current.session.phase)
  const plan = input => experimentPlan(input, options)

  function publish(run, events = [], issues = []) {
    const payload = JSON.stringify({ session: run.session, events, issues, stderr: run.stderr, streamError: run.streamError })
    for (const client of clients) {
      if (client.writableEnded || client.destroyed) continue
      if (client.writableNeedDrain) client.end()
      else client.write(`event: update\ndata: ${payload}\n\n`)
    }
  }

  async function refresh(run, final) {
    let parsed = emptyLog()
    const previousError = run.streamError
    try {
      parsed = await run.readNew(final)
      run.streamError = ''
    } catch (error) {
      run.streamError = '实时事件读取失败：' + error.message
    }
    run.log.events.push(...parsed.events)
    run.log.issues.push(...parsed.issues)
    if (parsed.events.length || parsed.issues.length || run.dirty || previousError !== run.streamError) {
      run.dirty = false
      publish(run, parsed.events, parsed.issues)
    }
  }

  function killGroup(run) {
    if (!run.child.pid) return
    try { process.kill(-run.child.pid, 'SIGKILL') }
    catch (error) { if (error.code !== 'ESRCH') throw error }
  }

  async function start(input) {
    if (closing || starting || active()) throw problem(409, '已有实验正在运行或停止中')
    if (process.platform === 'win32') throw problem(409, '请在 WSL Debian 中启动 Dashboard 后运行实验')
    const launch = plan(input)
    starting = true
    try {
      await access(binary, constants.X_OK).catch(() => { throw problem(409, '没有可执行的 hashring-rs。请先在同一环境构建 release 二进制。') })
      await mkdir(resultsRoot, { recursive: true })
      await mkdir(launch.output).catch(error => {
        if (error.code === 'EEXIST') throw problem(409, '结果目录名已存在，请换一个名称')
        throw error
      })
      if (launch.configFile) {
        await mkdir(path.dirname(launch.configFile), { recursive: true })
        await writeFile(launch.configFile, launch.yaml, { flag: 'wx' })
      }
      if (closing) throw problem(409, 'Dashboard 服务正在关闭')
      const child = spawn(binary, launch.args, { cwd: repoRoot, detached: true, stdio: ['ignore', 'ignore', 'pipe'] })
      const run = {
        session: { id: launch.id, phase: 'starting', startedAt: Date.now(), endedAt: null, command: launch.command, message: '', hasResult: false },
        child, output: launch.output, log: emptyLog(), stderr: '', streamError: '', dirty: false,
        readNew: followEvents(path.join(launch.output, 'events.jsonl')),
      }
      current = run
      child.stderr.setEncoding('utf8').on('data', chunk => {
        run.stderr = (run.stderr + chunk).slice(-48000)
        run.dirty = true
      })
      child.once('error', error => { run.session.message = error.message })
      child.once('spawn', () => {
        if (run.session.phase !== 'starting') return
        run.session.phase = 'running'
        publish(run)
      })
      const timer = setInterval(() => {
        if (!run.poll) run.poll = refresh(run, false).finally(() => { run.poll = null })
      }, 350)
      run.done = new Promise(resolve => child.once('close', async (code, signal) => {
        clearInterval(timer)
        try {
          if (code !== 0) killGroup(run)
          await run.poll
          await refresh(run, true)
          run.session.hasResult = await access(path.join(run.output, 'manifest.json')).then(() => true, () => false)
          if (code === 0) {
            run.session.phase = 'succeeded'
          } else if (run.session.phase === 'stopping') {
            run.session.phase = 'stopped'
            run.session.message = '已停止，已写入的结果保留。'
          } else {
            run.session.phase = 'failed'
            run.session.message ||= '实验退出（' + (signal || code) + '），可查看 stderr。'
          }
        } catch (error) {
          run.session.phase = 'failed'
          run.session.message = error.message
        } finally {
          run.session.endedAt = Date.now()
          publish(run)
          resolve()
        }
      }))
      publish(run)
      return run.session
    } finally {
      starting = false
    }
  }

  function stop(id) {
    if (!active() || current.session.id !== id) throw problem(409, '该实验已结束或不是当前运行')
    if (current.session.phase !== 'stopping') {
      killGroup(current)
      current.session.phase = 'stopping'
      publish(current)
    }
    return current.session
  }

  function stream(res) {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      'X-Content-Type-Options': 'nosniff',
    })
    clients.add(res)
    const snapshot = { session: current?.session ?? null, log: current?.log ?? emptyLog(), stderr: current?.stderr ?? '', streamError: current?.streamError ?? '' }
    res.write(`event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n`)
    const heartbeat = setInterval(() => { if (!res.writableNeedDrain) res.write(': ping\n\n') }, 15000)
    res.once('close', () => { clearInterval(heartbeat); clients.delete(res) })
  }

  async function topology(id) {
    const run = current
    if (!active() || run.session.id !== id) throw problem(409, '当前没有可查询的运行中集群')
    const address = await coordinatorAddress(run)
    if (!address) throw problem(409, '等待协调器监听地址')
    const args = ['--coordinator', 'http://' + address, '--deadline-ms', '1500']
    try {
      const [ring, replicas] = await Promise.all(['topology', 'replica-status'].map(command =>
        execute(binary, [command, ...args], { cwd: repoRoot, timeout: 2400, maxBuffer: 8 * 1024 * 1024 }).then(result => parseRecordedJson(result.stdout))))
      run.coordinator = address
      return { observedAt: Date.now(), ring, replicas }
    } catch {
      run.coordinator = undefined
      throw problem(503, '暂时无法查询协调器，请稍后重试')
    }
  }

  return {
    preview(input) { const { id, command, script } = plan(input); return { id, command, script } },
    start, stop, stream, topology,
    status: () => current?.session ?? null,
    async close() {
      closing = true
      for (const client of clients) client.end()
      if (active()) { stop(current.session.id); await current.done }
    },
  }
}
