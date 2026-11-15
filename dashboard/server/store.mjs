import { readdir, readFile, realpath } from 'node:fs/promises'
import path from 'node:path'
import { parseEventLog, parseRecordedJson } from './events.mjs'

// 目录中不存在 summary 是正常情况：实验可能尚未结束。
async function readJson(dir, name) {
  try {
    const file = await containedFile(dir, name)
    const value = parseRecordedJson(await readFile(file, 'utf8'))
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      throw new Error('文件内容应为 JSON 对象')
    }
    return { value }
  }
  catch (error) {
    if (error.code === 'ENOENT') return { value: null }
    return { value: null, warning: `${name} 读取失败：${error.message}` }
  }
}

function isInside(base, target) {
  const relative = path.relative(base, target)
  return relative !== '' && relative !== '..' && !relative.startsWith('..' + path.sep) && !path.isAbsolute(relative)
}

// 结果和日志里的符号链接同样不能把读取引到允许目录之外。
async function containedFile(dir, name) {
  const base = await realpath(dir)
  const file = await realpath(path.join(base, name))
  if (!isInside(base, file)) throw Object.assign(new Error('文件路径超出允许目录'), { status: 400 })
  return file
}

export function createStore(resultsRoot) {
  const root = path.resolve(resultsRoot)
  async function directory(id) {
    const base = await realpath(root)
    const target = await realpath(path.resolve(root, id))
    if (!isInside(base, target)) {
      throw Object.assign(new Error('结果路径超出 results 目录'), { status: 400 })
    }
    return target
  }

  async function readRun(id) {
    const dir = await directory(id)
    const [manifest, summary] = await Promise.all([
      readJson(dir, 'manifest.json'), readJson(dir, 'summary.json'),
    ])
    const warnings = [manifest.warning, summary.warning].filter(Boolean)
    const m = manifest.value
    const s = summary.value
    const status = s?.success === true ? 'success' : s?.success === false ? 'failed' : 'incomplete'
    return {
      id, name: id, status, warnings,
      // 清单可能包含很大的源码快照；常规页面只传展示所需字段，原文按需读取。
      manifest: m ? {
        schema_version: m.schema_version, started_unix_ms: m.started_unix_ms,
        config: m.config, build: m.build, system: m.system,
        source_reproducible: m.source_reproducible,
      } : null,
      summary: s,
    }
  }

  async function listRuns() {
    const ids = []
    async function walk(dir) {
      const entries = await readdir(dir, { withFileTypes: true })
      if (entries.some(e => e.isFile() && ['manifest.json', 'summary.json'].includes(e.name))) {
        ids.push(path.relative(root, dir).split(path.sep).join('/'))
        return
      }
      for (const entry of entries) if (entry.isDirectory()) await walk(path.join(dir, entry.name))
    }
    try { await walk(root) }
    catch (error) { if (error.code !== 'ENOENT') throw error }
    const runs = await Promise.all(ids.filter(Boolean).map(async id => {
      let run
      try { run = await readRun(id) }
      catch (error) {
        return { id, name: id, status: 'incomplete', config: {}, warnings: [`结果读取失败：${error.message}`] }
      }
      return {
        id, name: id, status: run.status, warnings: run.warnings,
        config: run.manifest?.config ?? {},
        started_unix_ms: run.manifest?.started_unix_ms,
        elapsed_seconds: run.summary?.elapsed_seconds,
      }
    }))
    return runs.sort((a, b) => (b.started_unix_ms ?? 0) - (a.started_unix_ms ?? 0))
  }

  async function rawFile(id, filename) {
    if (!['manifest.json', 'summary.json', 'events.jsonl'].includes(filename)) {
      throw Object.assign(new Error('不支持的结果文件'), { status: 400 })
    }
    return readFile(await containedFile(await directory(id), filename), 'utf8')
  }

  async function logs(id, process) {
    const run = await directory(id)
    let dir
    try { dir = await realpath(path.join(run, 'process-logs')) }
    catch (error) { if (!process && error.code === 'ENOENT') return []; throw error }
    if (!isInside(run, dir)) throw Object.assign(new Error('日志路径超出目录'), { status: 400 })
    if (!process) {
      try { return (await readdir(dir, { withFileTypes: true })).filter(e => e.isFile() && e.name.endsWith('.log')).map(e => e.name) }
      catch (error) { if (error.code === 'ENOENT') return []; throw error }
    }
    if (path.basename(process) !== process || !process.endsWith('.log')) {
      throw Object.assign(new Error('不支持的日志名称'), { status: 400 })
    }
    const file = await containedFile(dir, process)
    return readFile(file, 'utf8')
  }

  async function events(id) {
    // 先检查运行目录：不存在的运行仍然是 404，而不是空时间线。
    const dir = await directory(id)
    try {
      const file = await containedFile(dir, 'events.jsonl')
      return { ...parseEventLog(await readFile(file, 'utf8')), missing: false }
    } catch (error) {
      if (error.code === 'ENOENT') return { events: [], issues: [], missing: true }
      throw error
    }
  }
  return { listRuns, readRun, rawFile, logs, events }
}
