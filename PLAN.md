# Initial Implementation Plan

## First implementation

Build a real distributed cache using independent processes as nodes. The
implementation should be location-agnostic: the same client abstraction and gRPC
contract must work whether a node is a local process, container, VM, or remote
server. Tests will use ordinary local processes.

One operational binary will support multiple roles:

- coordinator;
- data node;
- client/admin CLI.

A reusable Rust client library will back the CLI and test harness.

## Scope

The first pass includes:

- single-key `PUT` and `GET`;
- arbitrary byte keys and values;
- consistent hashing with virtual nodes;
- client-side routing;
- one durable coordinator;
- graceful scale-out and scale-in;
- online snapshot and changelog migration;
- one active topology change at a time;
- reproducible correctness and performance experiments.

It excludes:

- replication and failover;
- heartbeat eviction;
- automatic crash recovery;
- consensus or coordinator replication;
- `DELETE`;
- durable data-node storage;
- transactions and multi-key ordering.

Each key has exactly one owner. An unexpected owner failure makes its data
unavailable.

## Distribution boundary

Only the client/transport boundary is abstracted. This is not a broad framework
hiding distributed-system behavior.

Epochs, redirects, deadlines, migration state, and retryable failures remain
explicit. Unit tests may use an in-process client implementation, but integration
and acceptance tests must use real gRPC between separate processes.

## Topology

The coordinator publishes a canonical immutable topology snapshot containing:

- monotonic epoch;
- cluster hash seed;
- hash algorithm and encoding version;
- virtual-node configuration;
- physical members and endpoints;
- canonically ordered token assignments;
- snapshot digest.

Hash configuration is fixed when the cluster is created. The seed is persisted and
recorded in experiment manifests. Changing hashing parameters in a live cluster is
not supported initially.

The coordinator serializes topology changes. A batch may add or drain several
nodes, but another change cannot begin until the active one finishes or aborts.

## Ownership and migration

The source remains the sole authoritative owner throughout copying. A destination
accepts migration traffic but rejects normal client operations until ownership
transfers.

The lifecycle is:

1. Record the pending target topology.
2. Activate change capture for the moving hash ranges.
3. Copy a snapshot to the destination.
4. Replay the recorded changelog.
5. Continue replay until nearly caught up.
6. Temporarily reject writes only for the affected range.
7. Drain the remaining changelog.
8. Verify the destination.
9. Publish the new epoch and transfer ownership.
10. Retain the old copy until the coordinator explicitly confirms cleanup.

After publication, the former owner redirects all client operations. Retaining its
bytes does not permit it to serve stale reads.

Migration journals are bounded. Approaching the bound applies backpressure;
failure to catch up before a deadline aborts the attempt while the old topology
remains authoritative.

Destination verification uses:

- a contiguous final changelog watermark;
- record count;
- a deterministic strong digest over key, version, and value.

## Scale-in

Scale-in applies only to a reachable, cooperative node:

1. Mark it as draining.
2. Keep it authoritative while copying its ranges.
3. Replay concurrent writes.
4. Pause affected-range writes briefly.
5. Verify destinations.
6. Publish the target topology.
7. Redirect stale clients.
8. Clean up only after coordinator confirmation.
9. Stop the drained process.

Unexpected process loss is not treated as graceful drain.

## Request semantics

Single-key operations are linearizable while the coordinator and authoritative
owner are available. There is no ordering guarantee between different keys.

Record versions use:

```text
(topology_epoch, owner_sequence, owner_node_id)
```

Migration preserves versions. New writes after ownership transfer use the newer
topology epoch.

Logical operations have configurable overall deadlines. There are no infinite
retries.

During the short cutover pause, writes receive a retryable `RangeBusy` response
rather than entering an unbounded server queue. The client retries with bounded
jittered backoff.

A stale epoch does not automatically cause rejection:

- if the receiving node still owns the key, it processes the request and returns
  the current epoch;
- if ownership changed, it returns `Moved`;
- the client refreshes topology and retries within its deadline.

## Retry simplification

The first pass will not implement persistent request receipts, payload-digest
binding, migrated deduplication records, or an `InvalidRequestReuse` contract.
Request IDs may still be carried for tracing and correlation.

A retried `PUT` writes the same requested value but may allocate another version
if the original response was lost. A deadline may therefore return an unknown
write outcome. Stronger retry idempotence can be added later if experiments
demonstrate a need.

## Error contract

Protocol-level errors should include at least:

- `NotFound`;
- `Moved`;
- `RangeBusy`;
- `Unavailable`;
- `DeadlineExceeded`;
- `TooLarge`;
- `ResourceExhausted`;
- `InvalidArgument`.

Retryable outcomes and unknown-write outcomes must be explicitly documented.

## Persistence and interruption

The coordinator uses an embedded transactional store behind a repository
interface. It persists committed topology, pending topology, migration phase, and
per-range progress atomically.

Data nodes remain entirely in memory.

Before topology publication:

- source unavailable: block or abort; source remains recorded owner;
- destination unavailable: discard or restart its pending migration;
- coordinator restart: recover persisted state and restart copying when progress
  cannot be proven.

After publication, destination failure makes its range unavailable. The epoch is
not automatically rolled back.

## Initial scale target

Use small deterministic cases for correctness, followed by a nominal acceptance
profile of:

- 10 data-node processes;
- 1,000,000 logical keys;
- one coordinator process.

The scale may be adjusted after implementation based on actual machine capacity,
but every formal run must record its real configuration and preserve raw results.

## Future HA boundary

The first pass preserves only inexpensive extension points:

- versioned records;
- stable node IDs and separate process-instance IDs;
- immutable topology epochs;
- restartable migration states;
- isolated coordinator interfaces;
- placement policy separated from ring mechanics.

Replication factor, write quorum, failure model, fencing protocol, heartbeats,
repair, and consensus remain intentionally undecided.

## Unsettled implementation parameters

The following are implementation parameters rather than settled architectural
decisions:

- exact library versions;
- default virtual-node count;
- concrete hash implementation;
- key and value size limits;
- operation and migration timeouts;
- coordinator storage backend.
