#!/usr/bin/env python3
"""Compare a local Redis Cluster with the hashring-rs cluster behind its Redis gateway.

Both systems are driven with the same client binary (redis-benchmark) and the
same command templates, on the same host, one system at a time, so the only
difference between two numbers of the same case is the system under test.

The comparison is deliberately narrow:

  * A Redis Cluster is a client-routed cluster too, so the client talks to the
    masters directly. redis-benchmark cannot route by itself, so one benchmark
    process per master is used, each with a key template whose hash tag belongs
    to that master. The aggregate of those processes is the cluster's number.
  * The hashring-rs gateway routes by the ring, so one benchmark process per
    gateway is used, each with the same key template. The aggregate of those
    processes is the cluster's number.

Writes use a random key space, so every write is a new key at first. Reads
run after the writes of the same repetition, so they hit keys that exist.

redis-benchmark has no time limit, only a request count, so every phase runs a
pilot first and then a measured phase long enough to cover the same wall-clock
time on both systems. The pilot doubles as the warmup. Each process gets a
request count in proportion to its connections, so the processes of a phase
finish together, and the throughput of a phase is the requests of all its
processes over the wall-clock time of the phase.
"""

import argparse
import csv
import hashlib
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
REDIS_SCRIPT = os.path.join(HERE, "redis-cluster.sh")
HASHRING_SCRIPT = os.path.join(HERE, "hashring-cluster.sh")


class Case:
    """One shape of cluster on both sides."""

    def __init__(self, name, redis_masters, redis_replicas, nodes, rf, gateways):
        self.name = name
        self.redis_masters = redis_masters
        self.redis_replicas = redis_replicas
        self.nodes = nodes
        self.rf = rf
        self.gateways = gateways

    def describe(self):
        return (
            f"{self.name}: redis {self.redis_masters} master(s) "
            f"+{self.redis_replicas} replica(s), hashring {self.nodes} node(s) "
            f"rf={self.rf}, {self.gateways} gateway(s)"
        )


CASES = {
    "3m": Case("3m", 3, 0, 3, 1, 1),
    "3m1r": Case("3m1r", 3, 1, 3, 2, 1),
    "3m-gw3": Case("3m-gw3", 3, 0, 3, 1, 3),
    "6m": Case("6m", 6, 0, 6, 1, 1),
    "6m1r": Case("6m1r", 6, 1, 6, 2, 1),
}


# ---------------------------------------------------------------- CRC16 ----

def crc16(data: bytes) -> int:
    """The CRC-16/XMODEM variant Redis uses for cluster slots."""
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def slot_of(tag: str) -> int:
    return crc16(tag.encode()) % 16384


def tag_for_slot_range(ranges, prefix="t"):
    """A hash tag whose slot falls inside one of the given (start, end) ranges."""
    for i in range(100000):
        tag = f"{prefix}{i}"
        slot = slot_of(tag)
        for start, end in ranges:
            if start <= slot <= end:
                return tag
    raise RuntimeError("no hash tag found for range")


# ------------------------------------------------------------- sampling ----

class Sampler(threading.Thread):
    """Samples CPU time and RSS of the server processes of one system."""

    def __init__(self, pids, interval=0.5):
        super().__init__(daemon=True)
        self.pids = list(pids)
        self.interval = interval
        self.stop_event = threading.Event()
        self.start_stats = {}
        self.end_stats = {}
        self.max_rss = 0

    @staticmethod
    def _read(pid):
        try:
            with open(f"/proc/{pid}/stat") as f:
                fields = f.read().rsplit(") ", 1)[1].split()
            utime = int(fields[11])
            stime = int(fields[12])
            rss_pages = int(fields[21])
            return utime + stime, rss_pages * os.sysconf("SC_PAGE_SIZE")
        except (FileNotFoundError, ProcessLookupError, IndexError, ValueError):
            return None

    def run(self):
        for pid in self.pids:
            stat = self._read(pid)
            if stat:
                self.start_stats[pid] = stat
        while not self.stop_event.is_set():
            for pid in self.pids:
                stat = self._read(pid)
                if stat:
                    self.end_stats[pid] = stat
                    self.max_rss = max(self.max_rss, stat[1])
            self.stop_event.wait(self.interval)

    def finish(self):
        self.stop_event.set()
        self.join(timeout=2)
        for pid in self.pids:
            stat = self._read(pid)
            if stat:
                self.end_stats[pid] = stat
        cpu = 0
        per_pid = {}
        for pid, stat in self.end_stats.items():
            start = self.start_stats.get(pid, (0, 0))[0]
            spent = max(0, stat[0] - start) / os.sysconf("SC_CLK_TCK")
            per_pid[pid] = round(spent, 2)
            cpu += spent
        return {
            "cpu_seconds": cpu,
            "cpu_seconds_per_pid": per_pid,
            "max_rss_bytes": self.max_rss,
            "pids": self.pids,
        }


# -------------------------------------------------------------- helpers ----

def run(cmd, **kwargs):
    proc = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc


def require_free_ports(ports):
    """Fails if anything listens on these ports. A listener there is not ours
    to stop, and a cluster that cannot bind its ports is a broken cluster."""
    busy = []
    for port in ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                busy.append(port)
    if busy:
        raise RuntimeError(f"ports already in use: {busy}; stop whatever listens there or choose other base ports")


def parse_csv(text):
    """The last CSV line redis-benchmark prints."""
    for line in reversed(text.strip().splitlines()):
        line = line.strip()
        if not line.startswith('"'):
            continue
        fields = next(csv.reader([line]))
        if len(fields) < 8:
            continue
        try:
            return {
                "title": fields[0],
                "rps": float(fields[1]),
                "avg_ms": float(fields[2]),
                "p50_ms": float(fields[4]),
                "p95_ms": float(fields[5]),
                "p99_ms": float(fields[6]),
                "max_ms": float(fields[7]),
            }
        except ValueError:
            continue
    return None


# ------------------------------------------------------------- clusters ----

def redis_cli(redis_bin, port, *args):
    return run([os.path.join(redis_bin, "redis-cli"), "-h", "127.0.0.1", "-p", str(port), *args]).stdout


def master_ranges(redis_bin, port):
    """The slot ranges the master on this port owns."""
    cli = os.path.join(redis_bin, "redis-cli")
    out = run([cli, "-h", "127.0.0.1", "-p", str(port), "--json", "cluster", "slots"]).stdout
    try:
        entries = json.loads(out)
    except json.JSONDecodeError:
        entries = None
    if entries is not None:
        ranges = []
        for entry in entries:
            start, end, master = entry[0], entry[1], entry[2]
            if int(master[1]) == port:
                ranges.append((int(start), int(end)))
        if ranges:
            return ranges
    # Older redis-cli versions print one field per line.
    out = run([cli, "-h", "127.0.0.1", "-p", str(port), "cluster", "nodes"]).stdout
    ranges = []
    for line in out.splitlines():
        fields = line.split()
        if len(fields) < 8 or "master" not in fields[2]:
            continue
        if not fields[1].split("@")[0].endswith(f":{port}"):
            continue
        for span in fields[8].split(","):
            if "-" in span:
                start, end = span.split("-")
            elif span.isdigit():
                start = end = span
            else:
                continue
            ranges.append((int(start), int(end)))
    return ranges


class RedisCluster:
    def __init__(self, args, case, work_dir, base_port):
        self.args = args
        self.case = case
        self.dir = os.path.join(work_dir, "redis")
        self.base_port = base_port
        self.masters = []
        self.replicas = []
        self.tags = []
        self.pids = []

    def start(self):
        total = self.case.redis_masters * (self.case.redis_replicas + 1)
        ports = range(self.base_port, self.base_port + total)
        # Every node also listens on its cluster bus port, the port plus 10000.
        require_free_ports([*ports, *(port + 10000 for port in ports)])
        # A fresh directory: a node that keeps the cluster configuration of an
        # earlier case is not empty, and the cluster cannot be created again.
        shutil.rmtree(self.dir, ignore_errors=True)
        run([
            "bash", REDIS_SCRIPT, "start", self.dir,
            str(self.case.redis_masters), str(self.case.redis_replicas),
            str(self.base_port),
        ], env={**os.environ, "REDIS_BIN_DIR": self.args.redis_bin})
        for i in range(total):
            port = self.base_port + i
            with open(os.path.join(self.dir, str(port), "redis.pid")) as f:
                self.pids.append(int(f.read().strip()))
        for i in range(self.case.redis_masters):
            port = self.base_port + i
            ranges = master_ranges(self.args.redis_bin, port)
            self.masters.append(port)
            self.tags.append(tag_for_slot_range(ranges))
        for i in range(self.case.redis_masters, total):
            self.replicas.append(self.base_port + i)

    def stop(self):
        run(["bash", REDIS_SCRIPT, "stop", self.dir], env={**os.environ, "REDIS_BIN_DIR": self.args.redis_bin})
        self.pids = []

    def dbsize(self):
        return sum(int(redis_cli(self.args.redis_bin, port, "dbsize")) for port in self.masters)


class HashringCluster:
    def __init__(self, args, case, work_dir, base_node_port, base_gateway_port):
        self.args = args
        self.case = case
        self.dir = os.path.join(work_dir, "hashring")
        self.base_node_port = base_node_port
        self.base_gateway_port = base_gateway_port
        self.gateways = []
        self.pids = []
        self.gateway_pids = []
        self.node_pids = []
        self.coordinator_pids = []
        self.settings = {}

    def start(self):
        require_free_ports([
            *range(self.base_node_port, self.base_node_port + self.case.nodes),
            *range(self.base_gateway_port, self.base_gateway_port + self.case.gateways),
            50050,
        ])
        # A fresh directory: the coordinator keeps its topology in a state
        # file, and a state file from an earlier case would keep that case's
        # replication factor and ignore the settings of this one.
        shutil.rmtree(self.dir, ignore_errors=True)
        run([
            "bash", HASHRING_SCRIPT, "start", self.dir,
            str(self.case.nodes), str(self.case.rf), str(self.case.gateways),
            str(self.base_node_port), str(self.base_gateway_port),
        ], env={**os.environ, "HASHRING_BIN": self.args.hashring_bin})
        for pid_name in os.listdir(self.dir):
            if pid_name.endswith(".pid"):
                with open(os.path.join(self.dir, pid_name)) as f:
                    pid = int(f.read().strip())
                self.pids.append(pid)
                if pid_name.startswith("gateway-"):
                    self.gateway_pids.append(pid)
                elif pid_name.startswith("node-"):
                    self.node_pids.append(pid)
                else:
                    self.coordinator_pids.append(pid)
        self.gateways = [self.base_gateway_port + g for g in range(self.case.gateways)]
        with open(os.path.join(self.dir, "settings")) as f:
            for line in f:
                key, _, value = line.strip().partition("=")
                if key:
                    self.settings[key] = value

    def stop(self):
        run(["bash", HASHRING_SCRIPT, "stop", self.dir])
        self.pids = []

    def dbsize(self):
        return int(redis_cli(self.args.redis_bin, self.gateways[0], "dbsize"))


# ----------------------------------------------------------- benchmarks ----

def aggregate(results, window):
    """The numbers of one phase. results holds (role, requests, parsed) per
    process. The rates are the completed requests over the wall-clock window
    of the whole phase: a process that finishes early adds its requests to
    that window and not a rate measured over its own shorter one."""
    parsed = [r for _, _, r in results]
    # How long each process ran, from its own request count and rate. A
    # spread far above 1 means the processes did not overlap for the whole
    # phase.
    elapsed = [requests / r["rps"] for _, requests, r in results if r["rps"] > 0]
    return {
        "rps": sum(requests for _, requests, _ in results) / window,
        "write_rps": sum(requests for role, requests, _ in results if role == "write") / window,
        "read_rps": sum(requests for role, requests, _ in results if role == "read") / window,
        "finish_spread": max(elapsed) / min(elapsed) if elapsed else 0,
        "worst": {
            "p50_ms": max(r["p50_ms"] for r in parsed),
            "p95_ms": max(r["p95_ms"] for r in parsed),
            "p99_ms": max(r["p99_ms"] for r in parsed),
            "max_ms": max(r["max_ms"] for r in parsed),
        },
        "per_process": parsed,
    }


# ----------------------------------------------------------------- main ----

def git_revision():
    try:
        revision = run(["git", "-C", REPO, "rev-parse", "HEAD"]).stdout.strip()
        dirty = run(["git", "-C", REPO, "status", "--porcelain", "--untracked-files=no"]).stdout.strip()
        return revision, bool(dirty)
    except Exception:
        return "unknown", None


def git_commit_time():
    try:
        return int(run(["git", "-C", REPO, "log", "-1", "--format=%ct"]).stdout.strip())
    except Exception:
        return None


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tool_version(path):
    try:
        return run([path, "--version"]).stdout.strip().splitlines()[0]
    except Exception:
        return "unknown"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", required=True, help="output directory")
    parser.add_argument("--cases", default="3m,3m1r,3m-gw3", help="comma separated case names")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=20, help="measured seconds per phase")
    parser.add_argument("--pilot-requests", type=int, default=100000, help="requests per process in the pilot")
    parser.add_argument("--clients", type=int, default=64, help="total clients across processes")
    parser.add_argument("--client-procs", type=int, default=2, help="benchmark processes per endpoint")
    parser.add_argument("--keyspace", type=int, default=300000, help="total distinct keys")
    parser.add_argument("--value-bytes", type=int, default=128)
    parser.add_argument("--redis-bin", default=os.environ.get("REDIS_BIN_DIR", "/usr/bin"))
    parser.add_argument("--hashring-bin", default=os.path.join(REPO, "target/release/hashring-rs"))
    parser.add_argument("--redis-base-port", type=int, default=7100)
    parser.add_argument("--node-base-port", type=int, default=50051)
    parser.add_argument("--gateway-base-port", type=int, default=6399)
    parser.add_argument("--keep", action="store_true", help="leave the clusters of the last case running")
    args = parser.parse_args()

    names = [n.strip() for n in args.cases.split(",") if n.strip()]
    for name in names:
        if name not in CASES:
            parser.error(f"unknown case {name}; known: {', '.join(CASES)}")

    output = os.path.abspath(args.output)
    if os.path.exists(output) and os.listdir(output):
        parser.error(f"{output} exists and is not empty")
    os.makedirs(output, exist_ok=True)
    work = os.path.join(output, "work")
    os.makedirs(work, exist_ok=True)

    # The binary carries no revision of its own, so the checkout's revision
    # describes it only if it was built from that checkout. The digest and
    # the build time identify the binary that actually ran.
    hashring_bin = os.path.abspath(args.hashring_bin)
    revision, dirty = git_revision()
    binary_mtime = int(os.path.getmtime(hashring_bin))
    commit_time = git_commit_time()
    if commit_time is not None and binary_mtime < commit_time:
        print(f"warning: {hashring_bin} is older than the checkout's last commit; "
              f"checkout_revision may not describe it", file=sys.stderr, flush=True)
    metadata = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "host_cpus": os.cpu_count(),
        "checkout_revision": revision,
        "checkout_dirty": dirty,
        "hashring_binary": hashring_bin,
        "hashring_binary_sha256": file_sha256(hashring_bin),
        "hashring_binary_mtime": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(binary_mtime)),
        "redis_benchmark": tool_version(os.path.join(args.redis_bin, "redis-benchmark")),
        "redis_server": tool_version(os.path.join(args.redis_bin, "redis-server")),
        "settings": {
            "reps": args.reps,
            "seconds_per_phase": args.seconds,
            "pilot_requests_per_process": args.pilot_requests,
            "total_clients": args.clients,
            "client_procs_per_endpoint": args.client_procs,
            "keyspace": args.keyspace,
            "value_bytes": args.value_bytes,
            "redis_base_port": args.redis_base_port,
            "node_base_port": args.node_base_port,
            "gateway_base_port": args.gateway_base_port,
            "keep": args.keep,
            "cases": names,
        },
        # What hashring-cluster.sh resolved for each case: worker threads and
        # the node and gateway budgets, defaults or environment overrides.
        "hashring_settings": {},
    }

    def write_metadata():
        with open(os.path.join(output, "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2)

    write_metadata()

    rows = []
    for name in names:
        case = CASES[name]
        print(f"=== case {case.describe()}", flush=True)
        redis = RedisCluster(args, case, work, args.redis_base_port)
        hashring = HashringCluster(args, case, work, args.node_base_port, args.gateway_base_port)
        try:
            redis.start()
            hashring.start()
            metadata["hashring_settings"][name] = hashring.settings
            write_metadata()
            # A cluster with a process that is not running is a broken cluster,
            # and a benchmark against it measures the breakage.
            for role_name, pids in (("redis", redis.pids), ("hashring", hashring.pids)):
                dead = [pid for pid in pids if not os.path.exists(f"/proc/{pid}")]
                if dead:
                    raise RuntimeError(f"{role_name} processes not running: {dead}")
            # Key templates. On the Redis side each master gets its own hash
            # tag, so the key belongs to that master's slot range and no client
            # is redirected. On the hashring side the tag is part of the key
            # name only; the gateway routes by the ring.

            # redis-benchmark expands only __rand_int__ in a custom command;
            # __data__ and -d apply to its built-in tests, so the value is
            # built here.
            value = "v" * args.value_bytes

            def key_template(role, tag):
                if role == "write":
                    return ["set", f"k:{{{tag}}}:__rand_int__", value]
                return ["get", f"k:{{{tag}}}:__rand_int__"]

            def system_layout(system):
                if system == "redis":
                    return redis.masters, redis.tags, redis.pids
                return hashring.gateways, [None] * len(hashring.gateways), hashring.pids

            def keyspace_for(system):
                if system == "redis":
                    return max(1, args.keyspace // len(redis.masters))
                return args.keyspace

            def start_process(port, role, tag, clients, keyspace, requests):
                cmd = [
                    os.path.join(args.redis_bin, "redis-benchmark"),
                    "-h", "127.0.0.1", "-p", str(port),
                    "-c", str(clients),
                    "-r", str(keyspace),
                    "-n", str(requests),
                    "--csv",
                ] + list(key_template(role, tag))
                return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            def process_layout(system, workload):
                """(port, tag, role, clients) per benchmark process.

                One benchmark process per endpoint and role, and a few of them
                per endpoint: a single redis-benchmark process tops out around
                130k requests per second, so one process per endpoint would
                measure the client instead of the server. The connections are
                split so that the total is --clients on both systems, whatever
                the number of endpoints; the remainder goes one connection at a
                time, alternating between the roles."""
                endpoints, tags, _ = system_layout(system)
                if workload == "mixed":
                    roles = ("write", "read")
                else:
                    roles = ("write" if workload == "write" else "read",)
                slots = [
                    (port, tag if tag is not None else "tag", role)
                    for _ in range(args.client_procs)
                    for port, tag in zip(endpoints, tags)
                    for role in roles
                ]
                if args.clients < len(slots):
                    raise RuntimeError(f"--clients {args.clients} is fewer than the {len(slots)} benchmark processes of {system} {workload}")
                base, extra = divmod(args.clients, len(slots))
                return [(port, tag, role, base + (1 if i < extra else 0)) for i, (port, tag, role) in enumerate(slots)]

            def phase(system, workload, requests_for):
                """Runs one phase on one system and returns its numbers.
                requests_for(role, clients) is the request count of one
                process."""
                _, _, pids = system_layout(system)
                layout = process_layout(system, workload)
                sampler = Sampler(pids)
                sampler.start()
                started = time.time()
                try:
                    procs = []
                    for port, tag, role, clients in layout:
                        requests = requests_for(role, clients)
                        procs.append((role, requests, start_process(port, role, tag, clients, keyspace_for(system), requests)))
                    results = []
                    for role, requests, proc in procs:
                        stdout, stderr = proc.communicate()
                        if proc.returncode != 0:
                            raise RuntimeError(f"redis-benchmark failed:\n{stdout}\n{stderr}")
                        parsed = parse_csv(stdout)
                        if parsed is None:
                            raise RuntimeError(f"no benchmark output:\n{stdout}\n{stderr}")
                        results.append((role, requests, parsed))
                    window = time.time() - started
                    out = aggregate(results, window)
                finally:
                    resources = sampler.finish()
                out["resources"] = resources
                out["elapsed_seconds"] = round(window, 2)
                out["requests_total"] = sum(requests for _, requests, _ in results)
                return out

            def record(system, workload, rep, result):
                # The CPU of the gateway and of the data nodes is split, so the
                # cost of the proxy in front of the cluster is visible.
                per_pid = result["resources"].get("cpu_seconds_per_pid", {})
                gateway_cpu = 0.0
                node_cpu = 0.0
                if system == "redis":
                    server_cpu = sum(per_pid.values())
                else:
                    gateway_cpu = sum(per_pid.get(pid, 0) for pid in hashring.gateway_pids)
                    node_cpu = sum(per_pid.get(pid, 0) for pid in hashring.node_pids)
                    server_cpu = sum(per_pid.values())
                row = {
                    "case": name,
                    "system": system,
                    "workload": workload,
                    "rep": rep,
                    "rps": round(result["rps"], 2),
                    "write_rps": round(result.get("write_rps", 0), 2),
                    "read_rps": round(result.get("read_rps", 0), 2),
                    "p50_ms": result["worst"]["p50_ms"],
                    "p95_ms": result["worst"]["p95_ms"],
                    "p99_ms": result["worst"]["p99_ms"],
                    "max_ms": result["worst"]["max_ms"],
                    "elapsed_seconds": result["elapsed_seconds"],
                    "finish_spread": round(result["finish_spread"], 3),
                    "requests_total": result["requests_total"],
                    "cpu_seconds": round(result["resources"].get("cpu_seconds", 0), 2),
                    "gateway_cpu_seconds": round(gateway_cpu, 2),
                    "node_cpu_seconds": round(node_cpu, 2),
                    "max_rss_bytes": result["resources"].get("max_rss_bytes", 0),
                }
                rows.append(row)
                print(
                    f"  {system:8s} {workload:6s} rep{rep} "
                    f"rps={row['rps']:>10.0f} p50={row['p50_ms']:.3f}ms "
                    f"p99={row['p99_ms']:.3f}ms {row['elapsed_seconds']:.1f}s "
                    f"cpu={row['cpu_seconds']:.1f}s "
                    f"(gw={row['gateway_cpu_seconds']:.1f}s node={row['node_cpu_seconds']:.1f}s)",
                    flush=True,
                )
                return row

            # Pilot: measures the throughput of one request count, so the
            # measured phase can run for the same wall-clock time on both
            # systems. It also warms the key space up.
            pilots = {}
            for system in ("redis", "hashring"):
                for workload in ("write", "read"):
                    result = phase(system, workload, lambda role, clients: args.pilot_requests)
                    pilots[(system, workload)] = result["rps"]
                    print(f"  pilot {system:8s} {workload:6s} rps={result['rps']:.0f}", flush=True)

            for rep in range(1, args.reps + 1):
                order = ["redis", "hashring"] if rep % 2 == 1 else ["hashring", "redis"]
                for workload in ("write", "read", "mixed"):
                    for system in order:
                        # A process gets the share of its role's pilot rate
                        # that its connections are of all connections, so
                        # every process, and in the mixed phase both roles,
                        # runs for about --seconds.
                        def requests_for(role, clients, system=system):
                            return max(1, int(pilots[(system, role)] * args.seconds * clients / args.clients))
                        result = phase(system, workload, requests_for)
                        record(system, workload, rep, result)
                        if workload == "write":
                            size = redis.dbsize() if system == "redis" else hashring.dbsize()
                            rows[-1]["keys_in_cluster"] = size
                            print(f"           keys in cluster: {size}", flush=True)
        finally:
            # --keep leaves only the last case running: the next case needs
            # the same ports, and its fresh directories would lose the PID
            # files of the processes of this one.
            if not (args.keep and name == names[-1]):
                hashring.stop()
                redis.stop()

    fields = [
        "case", "system", "workload", "rep", "rps", "write_rps", "read_rps",
        "p50_ms", "p95_ms", "p99_ms", "max_ms", "elapsed_seconds", "finish_spread", "requests_total",
        "cpu_seconds", "gateway_cpu_seconds", "node_cpu_seconds", "max_rss_bytes",
        "keys_in_cluster",
    ]
    with open(os.path.join(output, "measurements.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    summary = summarize(rows)
    with open(os.path.join(output, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))


def summarize(rows):
    """Median numbers per case, workload and system, plus the ratio."""
    groups = {}
    for row in rows:
        groups.setdefault((row["case"], row["workload"], row["system"]), []).append(row)
    summary = {}
    for (case, workload, system), items in sorted(groups.items()):
        entry = summary.setdefault(case, {}).setdefault(workload, {})
        rps = [i["rps"] for i in items]
        entry[system] = {
            "reps": len(items),
            "rps_median": round(statistics.median(rps), 1),
            "rps_min": round(min(rps), 1),
            "rps_max": round(max(rps), 1),
            "p50_ms_median": round(statistics.median([i["p50_ms"] for i in items]), 3),
            "p99_ms_median": round(statistics.median([i["p99_ms"] for i in items]), 3),
            "elapsed_seconds_median": round(statistics.median([i["elapsed_seconds"] for i in items]), 1),
            "cpu_seconds_median": round(statistics.median([i["cpu_seconds"] for i in items]), 1),
            "gateway_cpu_seconds_median": round(statistics.median([i.get("gateway_cpu_seconds", 0) for i in items]), 1),
            "node_cpu_seconds_median": round(statistics.median([i.get("node_cpu_seconds", 0) for i in items]), 1),
            "max_rss_bytes_median": int(statistics.median([i["max_rss_bytes"] for i in items])),
        }
    for case, workloads in summary.items():
        for workload, systems in workloads.items():
            if "redis" in systems and "hashring" in systems:
                systems["hashring_over_redis"] = round(
                    systems["hashring"]["rps_median"] / systems["redis"]["rps_median"], 4
                )
    return summary


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
