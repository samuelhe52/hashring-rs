#!/usr/bin/env python3
"""Usage: samply-buckets.py <profile.json.gz> [--top N] [--run-dir DIR [--phase initial_put|initial_get_verify]]
With --run-dir, only samples inside that phase's window (from events.jsonl) count. Default phase initial_put.
Samples whose leaf is a blocking wait (kevent, __psynch_cvwait, __ulock_wait, semaphore waits) are
reported as 'parked-at-sample' and excluded from the on-CPU views: their CPU delta was spent before the
thread parked, on an unknown stack. __psynch_mutexwait is also excluded but reported on its own line,
since it marks pthread mutex contention.
Weights every sample by its thread CPU delta, so idle/parked threads count nothing.
Process roles come from samply --include-args (process name contains the subcommand / --id).

Two views per role:
  self   : leaf frame bucket (syscall name, allocator, memcpy, crate of the leaf Rust frame)
  owner  : innermost frame on the stack that belongs to a 'owner' crate
           (hashring_*, h2, hyper, tonic, tower, prost, redb); tokio/mio/std/core/alloc/libsystem
           are transparent, so writev under h2 is charged to h2. No owner -> 'runtime'.
"""
import json, gzip, sys, bisect, re, collections, os
args = sys.argv[1:]
path = args[0]; top = 25; window = None
if '--top' in args: top = int(args[args.index('--top') + 1])
p = json.load(gzip.open(path))
if '--run-dir' in args:
    rd = args[args.index('--run-dir') + 1]
    phase = args[args.index('--phase') + 1] if '--phase' in args else 'initial_put'
    for line in open(os.path.join(rd, 'events.jsonl')):
        e = json.loads(line)
        if e['event'] == phase + '_complete':
            end = e['unix_ms'] - p['meta']['startTime']; window = (end - e['fields']['seconds'] * 1000, end)
    if window is None: sys.exit('phase not found in events.jsonl')
syms_path = re.sub(r'\.json(\.gz)?$', '.json.syms.json', path)
sy = json.load(open(syms_path))
strtab = sy['string_table']
libsyms = {}
for d in sy['data']:
    # known_addresses maps an address to an index into symbol_table as given.
    known = {a: d['symbol_table'][i]['symbol'] for a, i in d['known_addresses']}
    table = sorted((e['rva'], e['rva'] + e['size'], e['symbol']) for e in d['symbol_table'])
    libsyms[d['code_id'].upper()] = (known, table, [r[0] for r in table])
libs = p['libs']
def symbolize(lib_idx, addr):
    if lib_idx is None: return '?', '?'
    lib = libs[lib_idx]; name = lib['debugName']
    s = libsyms.get(lib['codeId'].upper())
    if not s: return name, '?'
    known, table, starts = s
    if addr in known: return name, strtab[known[addr]]
    i = bisect.bisect_right(starts, addr) - 1
    if i >= 0 and table[i][0] <= addr < table[i][1]: return name, strtab[table[i][2]]
    return name, '?'

OWNERS = [('hashring', re.compile(r'^<?hashring_')), ('h2', re.compile(r'^<?h2::')),
          ('hyper', re.compile(r'^<?hyper(_util)?::')), ('tonic', re.compile(r'^<?tonic::')),
          ('tower', re.compile(r'^<?tower(_http)?::')), ('prost', re.compile(r'^<?prost::')),
          ('redb', re.compile(r'^<?redb::'))]
def owner_of(fn):
    # generic impls look like "<hashring_node::Foo as tonic::...>::bar": the first path wins
    for name, rx in OWNERS:
        if rx.search(fn): return name
    return None
NET = {'writev', '__sendmsg', 'sendmsg', '__sendto', 'sendto', '__recvfrom', 'recvfrom', '__recvmsg',
       'recvmsg', 'read', '__read_nocancel', 'write', '__write_nocancel', 'readv', '__writev'}
def self_bucket(lib, fn):
    if lib == 'libsystem_kernel.dylib':
        if fn in NET or fn.lstrip('_') in {n.lstrip('_') for n in NET}: return 'sys:net ' + fn
        if 'kevent' in fn: return 'sys:kevent'
        if 'psynch' in fn or 'ulock' in fn: return 'sys:lock/park ' + fn
        return 'sys:other ' + fn
    if lib == 'libsystem_malloc.dylib': return 'malloc/free'
    if lib == 'libsystem_platform.dylib': return 'memcpy/platform ' + fn.split('$')[0]
    if lib.startswith('libsystem') or lib.startswith('libc++') or lib == 'dyld': return 'libsystem other (' + lib + ')'
    m = re.match(r'^<?([A-Za-z0-9_]+)::', fn)
    return 'rust:' + (m.group(1) if m else fn[:40])

def role_of(t):
    n = t.get('processName') or ''
    if re.search(r'--id', n) or re.search(r'\bnode\b', n): return 'node'
    if 'coordinator' in n: return 'coordinator'
    if 'experiment' in n: return 'driver'
    return 'other:' + n[:30]

PARKED = re.compile(r'kevent|__psynch_cvwait|__ulock_wait|semaphore_.*wait|__semwait|__select|mach_msg')
agg = collections.defaultdict(lambda: {'self': collections.Counter(), 'owner': collections.Counter(), 'tot': 0.0,
                                       'parked': 0.0, 'mutexwait': 0.0, 'pids': set(), 'secs': 0.0})
for t in p['threads']:
    if not (t.get('processName') or '').startswith('hashring-rs'): continue
    role = role_of(t); s = t['samples']; st = t['stackTable']; ft = t['frameTable']; fu = t['funcTable']; rt = t['resourceTable']
    cache = {}
    def frame_info(fr):
        r = cache.get(fr)
        if r is None:
            res = fu['resource'][ft['func'][fr]]
            lib_idx = rt['lib'][res] if res is not None and res >= 0 else None
            r = symbolize(lib_idx, ft['address'][fr]); cache[fr] = r
        return r
    scache = {}
    def stack_info(si):
        r = scache.get(si)
        if r is None:
            leaf = frame_info(st['frame'][si]); own = None; k = si
            while k is not None:
                o = owner_of(frame_info(st['frame'][k])[1])
                if o: own = o; break
                k = st['prefix'][k]
            kind = 'mutex' if leaf[1] == '__psynch_mutexwait' else ('parked' if leaf[0] == 'libsystem_kernel.dylib' and PARKED.search(leaf[1]) else 'cpu')
            r = (self_bucket(*leaf), own or 'runtime', kind); scache[si] = r
        return r
    deltas = s.get('threadCPUDelta') or []
    for i in range(s['length']):
        si = s['stack'][i]
        if si is None or not deltas or deltas[i] is None: continue
        if window and not (window[0] <= s['time'][i] <= window[1]): continue
        w = deltas[i] / 1e6
        if w <= 0: continue
        sb, ob, kind = stack_info(si)
        a = agg[role]; a['pids'].add(t['pid'])
        if kind == 'parked': a['parked'] += w; continue
        if kind == 'mutex': a['mutexwait'] += w; continue
        a['self'][sb] += w; a['owner'][ob] += w; a['tot'] += w
grand = sum(a['tot'] for a in agg.values())
dur = (window[1] - window[0]) / 1000 if window else None
print(f"on-CPU sampled {grand:.2f}s" + (f" over {dur:.2f}s window = {grand/dur:.2f} cores" if dur else ""))
for role, a in sorted(agg.items()):
    allw = a['tot'] + a['parked'] + a['mutexwait']
    print(f"\n### {role}  ({len(a['pids'])} proc)  on-CPU {a['tot']:.2f}s = {a['tot']/grand:.0%} of all on-CPU"
          + (f", {a['tot']/dur:.2f} cores" if dur else ""))
    print(f"  of this role's CPU-delta: parked-at-sample {a['parked']/allw:.1%}, mutexwait {a['mutexwait']/allw:.1%} (excluded below)")
    print("  owner view:")
    for k, v in a['owner'].most_common(): print(f"    {v/a['tot']:6.1%}  {k}")
    sysv = sum(v for k, v in a['self'].items() if k.startswith('sys:'))
    print(f"  self view (top {top}); libsystem_kernel leaf total {sysv/a['tot']:.1%}:")
    for k, v in a['self'].most_common(top): print(f"    {v/a['tot']:6.1%}  {k}")
