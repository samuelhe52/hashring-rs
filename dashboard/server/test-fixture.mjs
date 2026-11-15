import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

export async function fixture(t) {
  const temp = await mkdtemp(path.join(tmpdir(), 'hashring-dashboard-test-'))
  t.after(() => rm(temp, { recursive: true, force: true }))
  const root = path.join(temp, 'results')
  await mkdir(root)
  async function writeRun(id, manifest, summary) {
    const dir = path.join(root, id)
    await mkdir(dir, { recursive: true })
    if (manifest !== undefined) await writeFile(path.join(dir, 'manifest.json'), JSON.stringify(manifest))
    if (summary !== undefined) await writeFile(path.join(dir, 'summary.json'), JSON.stringify(summary))
    return dir
  }
  return { temp, root, writeRun }
}

export const manifest = {
  started_unix_ms: 1793122437701,
  config: { mode: 'latency', node_count: 3, key_count: 2000, desired_replication_factor: 3, write_ack_policy: 'OwnerOnly' },
  source_reproducible: false,
  runtime_source: { untracked_files: [{ content: 'large source snapshot' }] },
}
export const partial = {
  success: false, error: 'verification failed', elapsed_seconds: 12,
  measurements: { rate_0: { target_rate: 500, achieved_rate: 500, retries: 0, late: 0, latency_us: { get: { p99: 732, samples: 4527 } } } },
}
