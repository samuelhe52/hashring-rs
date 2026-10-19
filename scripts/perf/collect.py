#!/usr/bin/env python3
"""Usage: collect.py <out_root>  -> prints one TSV row per run (and writes <out_root>/runs.tsv)."""
import json, sys, os, glob, re
root = sys.argv[1]
cols = ['config', 'rep', 'ok', 'put_ops_s', 'get_ops_s', 'put_cores', 'put_driver_cores', 'put_node_cores_max',
        'put_sys_share', 'get_cores', 'get_driver_cores', 'repl_rejections', 'repl_retries', 'peak_pending_max',
        'owner_dedup_rej', 'owner_wait_s', 'owner_hold_s', 'owner_acq', 'follower_wait_s', 'follower_hold_s', 'ack_wait_s', 'ack_hold_s']
RPC = [('client_put_rpc', 'cput'), ('node_put_handler', 'nput'), ('owner_replication_rpc', 'repl_rpc'),
       ('node_replicate_batch_handler', 'repl_h'), ('client_get_rpc', 'cget'), ('node_get_handler', 'nget')]
for _, short in RPC: cols += [short + '_mean', short + '_p50', short + '_p99']
cols += ['batch_entries_mean', 'repl_rpc_count', 'full_rf_s', 'cputop_mean']
rows = []
for d in sorted(glob.glob(os.path.join(root, '*', 'rep*'))):
    if not os.path.isdir(d): continue
    cfg, rep = d.split(os.sep)[-2], d.split(os.sep)[-1][3:]
    r = dict.fromkeys(cols, ''); r['config'] = cfg; r['rep'] = rep
    try: s = json.load(open(os.path.join(d, 'summary.json')))
    except Exception: r['ok'] = 'no-summary'; rows.append(r); continue
    r['ok'] = str(s.get('success'))
    r['full_rf_s'] = round(s.get('observations', {}).get('initial_full_rf_seconds') or 0, 2)
    m = s.get('measurements', {})
    r['put_ops_s'] = round(m.get('initial_put', {}).get('operations_per_second', 0))
    r['get_ops_s'] = round(m.get('initial_get_verify', {}).get('operations_per_second', 0))
    np_ = s.get('observations', {}).get('node_pressure_after_initial_put') or {}
    def tsum(field, key):
        v = [n.get(field) for n in np_.values() if isinstance(n.get(field), dict)]
        return round(sum(x.get(key, 0) for x in v) / 1e9, 3) if v else ''
    r['repl_rejections'] = sum(n.get('replication_reservation_rejections', 0) for n in np_.values())
    r['repl_retries'] = sum(n.get('replication_rpc_retries', 0) for n in np_.values())
    r['peak_pending_max'] = max((n.get('replication_stream_peak_pending', 0) for n in np_.values()), default='')
    r['owner_dedup_rej'] = sum(n.get('owner_dedup_rejections', 0) for n in np_.values())
    r['owner_wait_s'] = tsum('owner_write_timing', 'wait_nanos'); r['owner_hold_s'] = tsum('owner_write_timing', 'hold_nanos')
    v = [n['owner_write_timing'].get('acquisitions', 0) for n in np_.values() if isinstance(n.get('owner_write_timing'), dict)]
    r['owner_acq'] = sum(v) if v else ''
    r['follower_wait_s'] = tsum('follower_write_timing', 'wait_nanos'); r['follower_hold_s'] = tsum('follower_write_timing', 'hold_nanos')
    r['ack_wait_s'] = tsum('ack_write_timing', 'wait_nanos'); r['ack_hold_s'] = tsum('ack_write_timing', 'hold_nanos')
    try:
        txt = open(d + '.cpu.txt').read()
        put, _, get = txt.partition('== initial_get_verify_complete')
        def tot(block, col):
            mm = re.search(r'TOTAL\s+user\s+([\d.]+)\s+sys\s+([\d.]+)\s+cores\s+([\d.]+)\s+sys_share\s+(\d+)%', block)
            return mm.group(3) if col == 'cores' else (mm.group(4) + '%')
        r['put_cores'] = tot(put, 'cores'); r['put_sys_share'] = tot(put, 'sys')
        r['put_driver_cores'] = re.search(r'experiment\s+user.*cores\s+([\d.]+)', put).group(1)
        r['put_node_cores_max'] = max(float(x) for x in re.findall(r'node:\S+\s+user.*cores\s+([\d.]+)', put))
        r['get_cores'] = tot(get, 'cores'); r['get_driver_cores'] = re.search(r'experiment\s+user.*cores\s+([\d.]+)', get).group(1)
    except Exception as e: r['put_cores'] = f'cpu-parse-error:{e}'
    try:
        for line in open(d + '.rpc.txt'):
            f = line.split()
            if not f or f[0] == 'histogram': continue
            for name, short in RPC:
                if f[0] == name: r[short + '_mean'], r[short + '_p50'], r[short + '_p99'] = f[3], f[4], f[6]
            if f[0] == 'replication_batch_entries': r['batch_entries_mean'] = f[3]
            if f[0] == 'owner_replication_rpc': r['repl_rpc_count'] = f[2]
            if f[0] == 'client_put_operation': r['cputop_mean'] = f[3]
    except FileNotFoundError: pass
    rows.append(r)
out = '\t'.join(cols) + '\n' + ''.join('\t'.join(str(r[c]) for c in cols) + '\n' for r in rows)
open(os.path.join(root, 'runs.tsv'), 'w').write(out); print(out, end='')
