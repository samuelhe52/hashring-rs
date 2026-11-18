import http from 'node:http'
import { execFile } from 'node:child_process'
import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { createStore } from './store.mjs'
import { createExperiments } from './experiments.mjs'

const dashboard = fileURLToPath(new URL('..', import.meta.url))
const repo = path.resolve(dashboard, '..')
const demoPrefix = '@demo/'
const evidenceFiles = new Map([
  ['acceptance-metrics.md', 'docs/acceptance-metrics.md'],
  ['scaling-under-load.md', 'docs/scaling-under-load.md'],
  ['fault-under-load.md', 'docs/fault-under-load.md'],
  ['latency-acceptance.md', 'docs/latency-acceptance.md'],
  ['latency-medians.tsv', 'docs/data/latency-acceptance/latency-acceptance-20261022-172621/medians.tsv'],
])

function demoRecord(run) {
  return { ...run, id: demoPrefix + run.id, source: 'demo' }
}

function send(res, status, body, type = 'application/json; charset=utf-8') {
  res.writeHead(status, { 'Content-Type': type, 'X-Content-Type-Options': 'nosniff' })
  res.end(type.startsWith('application/json') ? JSON.stringify(body) : body)
}

function snapshotOwnedKeys(binary, keyCount, raw) {
  return new Promise((resolve, reject) => {
    const child = execFile(binary, ['experiment-owned-keys', '--keys', String(keyCount)], { timeout: 30_000 }, (error, stdout) => {
      if (error) return reject(Object.assign(new Error('无法按此快照计算主节点键数'), { status: 422 }))
      try { resolve(JSON.parse(stdout)) }
      catch (failure) { reject(failure) }
    })
    // The exit callback handles launch and parsing failures, including early exits.
    child.stdin.on('error', () => {})
    child.stdin.end(raw)
  })
}

async function readCommand(req) {
  const host = req.headers.host ?? ''
  if (!/^(127\.0\.0\.1|localhost)(:\d+)?$/i.test(host) || (req.headers.origin && req.headers.origin !== 'http://' + host)) {
    throw Object.assign(new Error('仅允许从本机 Dashboard 发起实验操作'), { status: 403 })
  }
  if (!/^application\/json(?:\s*;|$)/i.test(req.headers['content-type'] ?? '')) {
    throw Object.assign(new Error('实验操作须发送 JSON'), { status: 415 })
  }
  const chunks = []
  let bytes = 0
  for await (const chunk of req.iterator({ destroyOnReturn: false })) {
    bytes += chunk.length
    if (bytes > 256 * 1024) throw Object.assign(new Error('配置不能超过 256 KiB'), { status: 413 })
    chunks.push(chunk)
  }
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8')) }
  catch { throw Object.assign(new Error('请求 JSON 无法解析'), { status: 400 }) }
}

export async function createDashboardServer({
  resultsRoot = process.env.RESULTS_DIR || path.join(repo, 'results'),
  binary = process.env.HASHRING_BIN || path.join(repo, 'target/release/hashring-rs'),
  dev = false,
} = {}) {
  const root = path.resolve(resultsRoot)
  const executable = path.resolve(repo, binary)
  const store = createStore(root)
  const demo = createStore(path.join(dashboard, 'demo'))
  const experiments = createExperiments({ resultsRoot: root, repoRoot: repo, binary: executable })
  const vite = dev ? await (await import('vite')).createServer({
    configFile: path.join(dashboard, 'vite.config.ts'),
    server: { middlewareMode: true }, appType: 'spa',
  }) : null
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://localhost')
      if (url.pathname.startsWith('/api/experiments')) {
        if (req.method === 'GET' && url.pathname === '/api/experiments/current') return send(res, 200, { session: experiments.status() })
        if (req.method === 'GET' && url.pathname === '/api/experiments/current/stream') return experiments.stream(res)
        if (req.method === 'GET' && url.pathname === '/api/experiments/current/topology') return send(res, 200, await experiments.topology(url.searchParams.get('id')))
        if (req.method === 'POST' && url.pathname === '/api/experiments/preview') return send(res, 200, experiments.preview(await readCommand(req)))
        if (req.method === 'POST' && url.pathname === '/api/experiments') return send(res, 201, await experiments.start(await readCommand(req)))
        if (req.method === 'POST' && url.pathname === '/api/experiments/current/stop') {
          const input = await readCommand(req)
          return send(res, 200, experiments.stop(input?.id))
        }
        return send(res, 404, { error: '接口不存在' })
      }
      if (url.pathname.startsWith('/api/')) {
        if (req.method !== 'GET') return send(res, 405, { error: '结果接口仅支持读取' })
        if (url.pathname === '/api/runs') {
          const includeDemo = url.searchParams.get('include') === 'demo'
          const [runs, samples] = await Promise.all([store.listRuns(), includeDemo ? demo.listRuns() : []])
          return send(res, 200, { runs: [...runs, ...samples.map(demoRecord)] })
        }
        if (url.pathname.startsWith('/api/evidence/')) {
          const file = evidenceFiles.get(url.pathname.slice('/api/evidence/'.length))
          if (!file) return send(res, 404, { error: '证据文件不存在' })
          return send(res, 200, await readFile(path.join(repo, file), 'utf8'), 'text/plain; charset=utf-8')
        }
        const route = /^\/api\/runs\/([^/]+)(?:\/(raw|logs|events|owned-keys))?$/.exec(url.pathname)
        if (!route) return send(res, 404, { error: '接口不存在' })
        let id
        try { id = decodeURIComponent(route[1]) }
        catch { return send(res, 400, { error: '运行名称编码无效' }) }
        const isDemo = id.startsWith(demoPrefix)
        const source = isDemo ? demo : store
        if (isDemo) id = id.slice(demoPrefix.length)
        if (route[2] === 'events') return send(res, 200, await source.events(id))
        if (route[2] === 'owned-keys') {
          const line = Number(url.searchParams.get('line'))
          if (!Number.isSafeInteger(line) || line < 1) return send(res, 400, { error: '缺少拓扑快照行号' })
          const [run, log] = await Promise.all([source.readRun(id), source.events(id)])
          const snapshot = log.events.find(event => event.line === line && event.event === 'topology_published')
          if (!snapshot) return send(res, 404, { error: '未找到这份拓扑快照' })
          const keys = run.manifest?.config?.key_count
          if (!Number.isSafeInteger(keys) || keys < 1) return send(res, 422, { error: '本次运行未记录初始键数' })
          return send(res, 200, await snapshotOwnedKeys(executable, keys, snapshot.raw))
        }
        if (route[2] === 'raw') {
          const filename = url.searchParams.get('file')
          const content = await source.rawFile(id, filename)
          if (url.searchParams.get('download') === '1') res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(filename)}`)
          return send(res, 200, content, 'text/plain; charset=utf-8')
        }
        if (route[2] === 'logs') {
          const process = url.searchParams.get('process')
          const content = await source.logs(id, process)
          if (process && url.searchParams.get('download') === '1') res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(process)}`)
          return send(res, 200, content, process ? 'text/plain; charset=utf-8' : 'application/json; charset=utf-8')
        }
        const run = await source.readRun(id)
        return send(res, 200, isDemo ? demoRecord(run) : run)
      }
      if (vite) return vite.middlewares(req, res)
      const file = url.pathname.startsWith('/assets/')
        ? path.join(dashboard, 'dist', 'assets', path.basename(url.pathname))
        : path.join(dashboard, 'dist/index.html')
      const content = await readFile(file)
      const mime = { '.js': 'text/javascript', '.css': 'text/css', '.html': 'text/html' }
      res.writeHead(200, { 'Content-Type': `${mime[path.extname(file)] || 'application/octet-stream'}; charset=utf-8` })
      res.end(content)
    } catch (error) {
      send(res, error.status ?? (error.code === 'ENOENT' ? 404 : 500), { error: error.message })
    }
  })

  return {
    server,
    async close() {
      await experiments.close()
      await vite?.close()
      await new Promise(resolve => server.close(resolve))
    },
  }
}

// 被测试文件 import 时不启动端口；直接执行此文件时才启动本地服务。
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const app = await createDashboardServer({ dev: process.argv.includes('--dev') })
  app.server.listen(Number(process.env.PORT || 4173), '127.0.0.1', () => {
    console.log('Hashring Dashboard: http://localhost:' + app.server.address().port)
    console.log('读取实验目录: ' + path.resolve(process.env.RESULTS_DIR || path.join(dashboard, '../results')))
  })
  app.server.on('error', async error => { console.error(error.message); process.exitCode = 1; await app.close() })
  process.once('SIGINT', () => app.close())
  process.once('SIGTERM', () => app.close())
}
