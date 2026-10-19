import json, sys
put_s, nodes = None, {}
for line in open(sys.argv[1]):
    e = json.loads(line)
    if e['event'] == 'initial_put_complete' and put_s is None:
        put_s = e['fields']['seconds']
    elif e['event'] == 'node_pressure_sampled' and not nodes:
        nodes = e['fields']['nodes']  # first sample: right after the initial load
def tot(k): return sum((n.get(k) or 0) for n in nodes.values())
def timing(p):
    g = lambda f: sum(((n.get(p) or {}).get(f) or 0) for n in nodes.values())
    return [g('acquisitions'), g('hold_nanos'), g('wait_nanos')]
row = [put_s, tot('replication_reservation_rejections'), tot('replication_rpc_retries')]
for p in ['owner_write_timing', 'follower_write_timing', 'ack_write_timing', 'ack_completion_write_timing']:
    row += timing(p)
print('\t'.join(str(x) for x in row))
