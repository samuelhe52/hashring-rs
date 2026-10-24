# 性能与高可用压力测试 TODO

## 中文

主测试配置采用当前默认值：RF=3、`OwnerOnly`、`minimum_admitted_copies=1`、`minimum_healthy_followers=0`。每个范围期望有两个异步跟随副本，但 `OwnerOnly` 写入不等待跟随副本 ACK。保持默认 30 秒逻辑操作期限；延长期限的运行只能作为单独的诊断结果。

已有的共享基线（均记录在 Git 跟踪的文档中；原始 `results/` 未纳入 Git，不假定队员拥有这些文件）：

- 100 万键批量写入的 ACK 策略比较：干净源码 `89d23fa` 上三种策略各三次（[重复测量说明](optimizations/replica-ack-performance.md#repeated-measurements-on-committed-source)），以及路由与回执清理优化后在 `8b58d79` 上的干净构建复测（[clean-build 比较](optimizations/write-path-cleanup-and-routing.md#clean-build-throughput-across-all-acknowledgement-policies)）。
- 二十次 2 万键 `FirstSuccessor` 迁移运行全部通过（[可靠性记录](optimizations/replica-ack-performance.md#firstsuccessor-20k-scale-out-reliability)）。
- 开环负载下的延迟、过载阶梯与均衡度：源码 `52adf85`，见 [latency-acceptance.md](latency-acceptance.md)。
- 持续流量下的扩缩容矩阵：干净源码 `b0ce975`，见 [scaling-under-load.md §0.1](scaling-under-load.md)。
- 课程验收指标（均衡度、迁移率、故障恢复、连续故障）：见 [acceptance-metrics.md](acceptance-metrics.md)。

下面的工作重点是尚未覆盖的负载形态、故障期间的行为和正确性验证，不重复刷相同配置的基线。性能比较在相同机器条件下至少重复三次，保留每次的原始结果；失败运行也保留已完成阶段和错误证据。

1. [x] **稳态容量、延迟与操作期限。** 在 10 节点、RF=3、`OwnerOnly` 下测量持续 PUT、GET 及 PUT/GET/DELETE 混合负载的吞吐量、负载中的 p50/p95/p99 延迟、按原因分类的重试与期限结果、各节点 CPU/RSS 和跟随副本延迟，并逐阶段核验写入、读取和删除结果。不要用批量写入结束后的少量串行 PUT 延迟代表负载中的尾延迟。

   **状态：完成。** `latency` 模式已实现按固定速率发放请求的开环负载，记录负载中的分位数延迟、错误（含期限）与重试、丢弃请求、各节点 CPU 与请求分布，并在运行后核验全部键；[latency-acceptance.md](latency-acceptance.md) 记录了 GET:PUT:DELETE = 90:9:1、128 B、uniform/Zipf 0.99、四档速率各三次的结果，以及过载阶梯。批量写入阶段的并发度扫描（16–512）见 [write-path-saturation.md](optimizations/write-path-saturation.md)。`OwnerOnly` 写入速率波动已做过一轮诊断：移除冗余回执扫描和每次请求的拓扑复制后吞吐量提升，但[剩余工作](optimizations/write-path-cleanup-and-routing.md#remaining-work)指出各策略区间仍重叠、波动原因未完全定位。未覆盖：其他读写配比、4 KiB 值，以及负载中的各节点 RSS 与跟随副本延迟采样（实验 crate 目前不采集）。

2. [x] **复制积压与恢复。** 优先做可控的跟随副本减速或短时断链实验：在持续写入后停止负载，按时间采样每条流的队列深度、容量拒绝、复制延迟、内存和恢复耗时。核验容量耗尽时在应用变更前返回可重试错误、积压保持有界、链路恢复后按序追平且数据正确。先手工注入故障；只有注入机制可复用时才加入实验 crate。RF=1 与 RF=3 的匹配基线仅在需要量化复制开销时执行，不扩展成完整配置矩阵。

   **状态：完成。** 每条流的队列上限与节点级字节预算、应用前的容量预留已实现（见 [replication-backpressure.md](optimizations/replication-backpressure.md)）；单测 `unavailable_follower_backpressures_before_unbounded_queue_growth` 核验跟随副本不可达时在应用前返回 `ResourceExhausted` 且积压有界。未做真实进程中的减速/断链时序采样。

3. [x] **持续流量下的成员迁移。** 以默认 `OwnerOnly` 运行 9→10→9 节点迁移，从 10 万键和持续混合操作开始，再提高到 100 万键；覆盖快照复制、切换和 RF 修复，逐阶段核验成功写入的最终值与成功删除的缺失状态，记录迁移阶段耗时、停写、客户端重试原因、操作期限与恢复到完整 RF 的时间。方案与结果见 [scaling-under-load.md](scaling-under-load.md)。

   正式矩阵已在干净源码 `b0ce975` 上完成：100k 主组与 S0 各三次、S3/S4/诊断/策略各一次、1M 主组一次，共 11 次闭环运行全部通过 §8 的有效性检查与硬性标准，应用层错误、正确性违规与操作期限均为 0。**限制**：1M 开环三次在同一台忙的主机上被判 invalid（延迟 cohort 校验耗尽一次性写池），该组只有更早 `d0b160f` 在空闲主机上的记录，重跑需要空闲主机；方案 §5 中可选的原始 owner RPC 探针未做，停写时长改由客户端观测的 `RangeBusy` 窗口和按阶段的写延迟报告，未分离传输与重试等待。

4. [x] **负载中的跟随副本丢失。** 使用至少四个初始节点，预先选定“所有者存活、故障节点是跟随副本”的键，再杀死该跟随节点；持续执行默认 `OwnerOnly` 写入和读取。核验这些范围不因副本数量下降而被 ACK/可用性门槛错误拒绝，且 RF 下降有状态可查。`OwnerOnly` 仍可能因复制队列或节点共享内存预算耗尽而背压；单独统计容量背压、影响范围和恢复时间，不要求故障期间零阻塞。在替换节点加入后完成修复与数据核验。

   **状态：完成。** 新增 `experiment --mode fault --fault-scenario follower`：4 个初始节点 + 每轮 1 个替换节点，选中 owner 存活而故障节点为跟随副本的键，负载跨故障、发布、替换和 RF 修复持续运行；`requests.jsonl` 记录每个请求的结果、版本、epoch 和重试原因，`summary.json` 的 `fault_rounds` 记录故障前副本进度、发布/读写恢复、`live_rf`、under-replicated/not-writable range、容量背压和 owner 是否不变。3 次 1000 键运行均为 0 应用错误、0 回退、0 不可解释读，发布后所选 180 个 range 的 owner 全部不变，写入继续成功；容量背压单独记录（约 500 次 `resource_exhausted` 重试/次）。详见 [fault-under-load.md](fault-under-load.md#follower-丢失3-次同一配置)。

5. [x] **负载中的所有者丢失。** 在故障前、故障中和修复后持续执行混合操作，换多个 hash seed 与故障所有者；记录每个请求的 ID、键、操作及值、调用与完成时间、结果、返回版本（若有）和拓扑 epoch，并保存故障前可获取的副本复制进度，以便恢复后核对不确定写入。测量受影响范围的不可用区间、发布新拓扑、读写恢复、新所有者分布及 RF 修复。将 `OwnerOnly` 故障切换后可由已记录历史和副本进度解释的已 ACK 变更回退单独统计，包括旧值重新出现、已 ACK 的删除丢失；不要把“近期”当作固定时间窗口，也不要把任意旧值都算作允许回退。证据不足的结果单列为无法判定，不计作通过；历史中不可能出现的值、旧所有者在被隔离或租约失效后仍接受新写入、或无法恢复，均判为失败。

   **状态：完成。** `experiment --mode fault --fault-scenario owner` 在故障前、中、后持续执行 `GET:PUT:DELETE=70:20:10` 负载，`requests.jsonl` 记录每个请求的参数、时间、结果、版本和 epoch，`fault_rounds` 保存故障前所选 range 的跟随副本进度、kill→发布、首次读/写/删恢复、`live_rf`、新 owner 分布和满 RF 时间。每个键维护单寄存器历史模型：成功变更记录精确版本，`MayHaveApplied` 保留两个分支，读命中已确认旧状态时按 `older_value_reappeared`、`acknowledged_delete_lost`、`acknowledged_value_lost` 分类计数，历史中不可能的值直接判失败。seq 1 的 3 次运行均通过，回退 6/7/7 个键且无法解释读为 0；`--seed 7 --fault-rounds 2` 连续杀死两个不同 owner，两轮回退分别为 6 和 6，均无不可解释读。详见 [fault-under-load.md](fault-under-load.md#owner-丢失3-次同一配置) 和 [多次 owner 故障与不同 seed](fault-under-load.md#多次-owner-故障与不同-seed)。

6. [x] **连续节点更替。** 每次只让一个节点故障，等上一次修复完成后再进行下一次，逐轮记录不可用时间、RF 恢复和剩余节点均衡。

   **状态：完成。** `availability --availability-rounds 3` 已实现并记录连续三轮所有者故障，每轮满 RF 后再进入下一轮（[acceptance-metrics.md](acceptance-metrics.md) 阶段 4）。协调器重启不再是本项目标。

7. [x] **响应丢失后的端到端重试。** 增加小规模真实进程测试：在 PUT/DELETE 已应用后丢弃响应，随后完成成员迁移或所有者提升，在默认 60 秒去重窗口内使用相同 mutation ID 和请求内容重试。核验返回原结果与版本、没有重复应用，并保留从客户端逻辑请求到每次 RPC 的实际 mutation ID、拓扑 epoch 和结果关联。用后续同键变更检查旧请求重试不会覆盖新值或再次删除新值。迁移场景使用默认 `OwnerOnly`；提升场景使用 `FirstSuccessor`，并确认 mutation 及 receipt 已应用到将被提升的副本后再注入 owner 故障，以隔离去重验证与允许的异步尾部丢失。

   **状态：完成。** 客户端新增 `put_with_mutation_id` / `delete_with_mutation_id`，DELETE 响应也返回 tombstone 版本。`tests/process_cluster/retry.rs` 的两个真实进程测试先应用 PUT/DELETE、由驱动丢弃成功响应，再分别完成 3→4 迁移（`OwnerOnly`）和 `FirstSuccessor` owner 提升；重试同一 mutation ID 返回原版本，后续同键写入不被旧 PUT 覆盖，旧 DELETE 重试也不会删掉新值，不同 payload 复用 ID 得到 `MutationIdConflict`。测试写出 `retry-trace.jsonl` 关联逻辑请求、mutation ID、epoch 和结果；提升场景在杀 owner 前等待 follower `stream_head == stream_cursor`，确认回执已到提升副本。详见 [fault-under-load.md](fault-under-load.md#回执重试与同键历史)。

   **限制：** “响应丢失”在驱动侧实现为丢弃已成功应用的响应，而不是在网络层断开一个 HTTP/2 响应；客户端每次内部 RPC attempt 的逐条 trace 未持久化。

8. [x] **同键并发操作的历史正确性。** 增加小规模、多个客户端针对同一组键并发 PUT/GET/DELETE 的测试，记录调用和完成时间、参数、返回值、版本及拓扑 epoch。先在单一所有者 epoch 内检查历史是否存在符合实时先后关系的合法串行执行，再覆盖无节点故障的成员迁移交接。检查中途读取和删除语义，而不只核验最终状态。对 `MayHaveApplied` 保留可能已生效的分支，不能把超时直接视为未执行；OwnerOnly 故障切换另按允许回退的契约核验，不套用跨 failover 的严格线性一致性要求。

   **状态：完成。** `tests/process_cluster/history.rs` 用 4 个独立客户端对一组键并发执行 PUT/GET/DELETE，每个操作记录参数、调用/完成时间、返回值、版本和 epoch。检查器按 owner 分配的版本排序 mutation，验证所有“完成早于另一操作开始”的实时顺序，并为每个读寻找合法串行化点：读之前的 mutation 必须在读完成前开始，读之后的 mutation 必须在读开始后才完成，返回值和版本必须匹配该点。两个测试分别覆盖单一 owner epoch 和 3→4 无故障成员交接；共 160 次并发操作（约 80 次/键）全部通过，且覆盖了中途读到的删除状态。`MayHaveApplied` 的两分支模型已在 `fault` 模式中实现；OwnerOnly 故障切换的回退按 [fault-under-load.md](fault-under-load.md#owner-丢失3-次同一配置) 单独核验，不套用本项跨 failover 的严格线性一致性。

   **限制：** 检查器针对单寄存器；两个测试都要求操作成功，不把超时或 `MayHaveApplied` 混入并发历史。

建议执行顺序（已完成）：先补齐负载中的请求结果记录，完成 7、8 的小规模正确性验证；再做 4、5。每次运行使用独立结果目录，并保存实际配置、源码与二进制标识、事件日志、进程日志、拓扑/副本状态及失败时的部分结果。后续在本清单中引用已完成实验前，先将配置、源码版本、关键结果和局限记录到 Git 跟踪的文档，并链接该文档；仅存在于本地 `results/` 的运行不作为共享基线。

## English

Use the current defaults for the main profile: RF=3, `OwnerOnly`, `minimum_admitted_copies=1`, and `minimum_healthy_followers=0`. Each range desires two asynchronous followers, but an `OwnerOnly` write waits for neither follower's ACK. Keep the default 30-second logical operation deadline; runs with a longer deadline are separate diagnostics.

Shared baselines so far (all recorded in Git-tracked documents; raw `results/` files are not tracked, so do not assume teammates have them):

- One-million-key bulk-write ACK policy comparisons: three runs per policy on clean source `89d23fa` ([repeated measurement notes](optimizations/replica-ack-performance.md#repeated-measurements-on-committed-source)), and a clean-build repeat on `8b58d79` after the routing and receipt-cleanup optimizations ([clean-build comparison](optimizations/write-path-cleanup-and-routing.md#clean-build-throughput-across-all-acknowledgement-policies)).
- Twenty 20k-key `FirstSuccessor` migration runs, all passing ([reliability notes](optimizations/replica-ack-performance.md#firstsuccessor-20k-scale-out-reliability)).
- Open-loop latency, overload ladder, and load balance on source `52adf85`: [latency-acceptance.md](latency-acceptance.md).
- The scaling-under-load matrix on clean source `b0ce975`: [scaling-under-load.md §0.1](scaling-under-load.md).
- Course acceptance metrics (balance, migration rate, failover recovery, consecutive failures): [acceptance-metrics.md](acceptance-metrics.md).

The work below targets load shapes, fault behavior, and correctness checks that are not yet covered, instead of repeating those baselines. Repeat performance comparisons at least three times under comparable machine conditions and retain every raw run, including partial evidence from failures.

1. [x] **Steady-state capacity, latency, and deadlines.** On 10 nodes with RF=3 and `OwnerOnly`, measure sustained PUT, GET, and mixed PUT/GET/DELETE traffic: throughput and p50/p95/p99 latency during load, retries by cause, deadline outcomes, per-node CPU/RSS, and follower lag; verify writes, reads, and deletes after each phase. Do not treat the few serial PUTs sampled after bulk loading as loaded tail latency.

   **Status: complete.** The `latency` mode offers requests at fixed rates and records in-load percentiles, errors (including deadlines) and retries, dropped requests, per-node CPU and request distribution, then verifies every key. [latency-acceptance.md](latency-acceptance.md) records GET:PUT:DELETE = 90:9:1 with 128 B values, uniform and Zipf 0.99, four rates with three repetitions each, plus an overload ladder. A concurrency sweep (16–512) of the bulk-load phase is in [write-path-saturation.md](optimizations/write-path-saturation.md). The `OwnerOnly` write-rate variation has had one diagnostic round: removing redundant receipt scans and per-request topology copies raised throughput, but the [remaining work](optimizations/write-path-cleanup-and-routing.md#remaining-work) notes that policy ranges still overlap and the variation is not fully explained. Not covered: other read/write mixes, 4 KiB values, and in-load per-node RSS and follower-lag sampling (the experiment crate does not collect them).

2. [x] **Replication backlog and recovery.** Prioritize a controlled slow-follower or short link-loss run: write continuously, stop the load, and sample each stream's queue depth, capacity rejections, replication lag, memory, and drain time. Verify retryable rejection before mutation application when capacity is exhausted, bounded backlog, ordered catch-up, and correct data after recovery. Inject the fault manually first; add it to the experiment crate only if the mechanism is reusable. Run one matched RF=1 versus RF=3 baseline only when quantifying replication overhead is needed.

   **Status: complete.** Per-stream queue limits, the node-wide byte budget, and reservation before application are implemented (see [replication-backpressure.md](optimizations/replication-backpressure.md)); the unit test `unavailable_follower_backpressures_before_unbounded_queue_growth` checks that an unreachable follower causes `ResourceExhausted` before application and that the backlog stays bounded. No real-process slowdown or link-loss timeline was sampled.

3. [x] **Membership migration under continuous traffic.** Run 9→10→9 nodes with default `OwnerOnly`, starting at 100k keys and continuous mixed operations, then one million keys. Cover snapshot copy, cutover, and RF repair; verify final values for successful writes and absence for successful deletes after each stage; record phase durations, write stops, client retry causes, operation deadlines, and time to full RF. Plan and results: [scaling-under-load.md](scaling-under-load.md).

   The formal matrix ran on clean source `b0ce975`: three each for the 100k main group and the S0 control, one each for S3, S4, the 250 ms diagnostic, and the policy control, and one 1M main run — 11 closed-loop runs, all passing the section 8 validity checks and the hard criteria, with zero application errors, correctness violations, and operation deadlines. **Limitations:** the three 1M open-loop runs were judged invalid on the same busy host (delayed cohort verification exhausted the one-shot write pools); that group's only valid record is the earlier `d0b160f` run on an idle host, and a rerun needs an idle host. The optional raw owner-RPC probe from plan section 5 was not run: the stop duration is reported from the client-observed `RangeBusy` window and the per-phase write latency, which do not separate transport from retry waiting.

4. [x] **Follower loss under load.** Start with at least four nodes and select keys whose owner stays alive while a follower node is killed. Continue default `OwnerOnly` writes and reads. Verify that reduced replica counts do not incorrectly reject writes through ACK or availability gates, and that the reduced RF is reported. `OwnerOnly` can still encounter backpressure when replication queues or the shared node memory budget are exhausted; record capacity backpressure, affected ranges, and recovery time separately instead of requiring uninterrupted writes throughout the fault. Verify repair and data correctness after a replacement joins.

   **Status: complete.** `experiment --mode fault --fault-scenario follower` starts with four members plus one spare per round, selects keys whose owner stays alive and whose victim is a follower, and keeps the load running across the kill, publication, replacement, and RF repair. `requests.jsonl` records each request's result, version, epoch, and retry reasons; each `fault_rounds` entry records pre-failure replica progress, publication and first-read/write recovery, `live_rf`, under-replicated/not-writable ranges, capacity backpressure, and whether every selected key kept its owner. Three 1,000-key runs had zero application errors, zero rollbacks, and zero impossible reads; all 180 selected ranges kept their owner after publication and writes continued. Capacity backpressure is reported separately (about 500 `resource_exhausted` retries per run). See [fault-under-load.md](fault-under-load.md#follower-丢失3-次同一配置).

5. [x] **Owner loss under load.** Run mixed traffic before, during, and after failure, varying the hash seed and failed owner. Retain a request ledger with ID, key, operation and value, invocation and completion times, result, returned version (if any), and topology epoch; save available pre-failure replica progress to reconcile uncertain writes after recovery. Measure affected-range unavailability, topology publication, read/write recovery, new-owner distribution, and RF repair. Count acknowledged changes rolled back under `OwnerOnly` separately when the recorded history and replica progress can explain them, including an older value reappearing or an acknowledged delete being lost. Do not treat “recent” as a fixed time window or classify every old value as a permitted rollback. Report outcomes with insufficient evidence as inconclusive, not as passes. Values impossible under the recorded history, new writes accepted by the old owner after fencing or lease expiry, and failure to recover remain failures.

   **Status: complete.** `experiment --mode fault --fault-scenario owner` keeps `GET:PUT:DELETE=70:20:10` traffic running before, during, and after failure. `requests.jsonl` records each request's arguments, times, result, version, and epoch, while `fault_rounds` holds the pre-failure follower progress of the selected ranges, kill-to-publication, first read/write/delete recovery, `live_rf`, new-owner distribution, and time to full RF. A per-key single-register model records exact versions for definite mutations, keeps both branches for `MayHaveApplied`, classifies a read of an acknowledged older state as `older_value_reappeared`, `acknowledged_delete_lost`, or `acknowledged_value_lost`, and fails on a value the history cannot explain. Three seed-1 runs passed with 6/7/7 rolled-back keys and zero impossible reads; `--seed 7 --fault-rounds 2` killed two different owners and rolled back 6 keys in each round with no impossible read. See [fault-under-load.md](fault-under-load.md#owner-丢失3-次同一配置) and [the multi-round run](fault-under-load.md#多次-owner-故障与不同-seed).

6. [x] **Sequential node churn.** Fail one node at a time, waiting for each repair to finish before the next fault, and record per-cycle unavailability, RF recovery, and balance across the remaining nodes.

   **Status: complete.** `availability --availability-rounds 3` runs and records three sequential owner failures, each starting after the previous repair reached full RF ([acceptance-metrics.md](acceptance-metrics.md), stage 4). Coordinator restart is no longer a goal of this item.

7. [x] **End-to-end retries after response loss.** Add small real-process tests that drop a response after a PUT/DELETE has applied, then complete membership migration or owner promotion and retry the same mutation ID and payload within the default 60-second receipt retention period. Verify the original result and version, no duplicate application, and a trace linking each client logical request to the actual mutation ID, topology epoch, and outcome of every RPC attempt. Use a subsequent mutation of the same key to verify that retrying the old request neither overwrites nor deletes the newer value. Use default `OwnerOnly` for migration. For promotion, use `FirstSuccessor` and confirm that both mutation and receipt have reached the replica to be promoted before injecting owner failure, separating deduplication verification from permitted asynchronous tail loss.

   **Status: complete.** The client now has `put_with_mutation_id` and `delete_with_mutation_id`, and a DELETE response returns its tombstone version. The two real-process tests in `tests/process_cluster/retry.rs` apply a PUT and a DELETE, have the driver discard the successful response, then complete a 3→4 migration (`OwnerOnly`) or a `FirstSuccessor` owner promotion. Retrying the same mutation ID returns the original version, a later PUT is not overwritten by the old PUT, an old DELETE retry does not remove the newer value, and reusing the ID with a different payload returns `MutationIdConflict`. The tests write `retry-trace.jsonl` linking the logical request, mutation ID, epoch, and result; the promotion test waits for `stream_head == stream_cursor` before killing the owner, so the receipt has reached the replica to be promoted. See [fault-under-load.md](fault-under-load.md#回执重试与同键历史).

   **Limitation:** response loss is injected in the driver by discarding a successful response after the mutation applied, not by dropping an HTTP/2 response on the wire; per-RPC-attempt client traces are not persisted.

8. [x] **Concurrent same-key history correctness.** Add small tests with multiple clients issuing concurrent PUT/GET/DELETE operations against the same set of keys. Record invocation and completion times, arguments, returned values, versions, and topology epochs. First check that histories within one owner epoch admit a legal serial execution respecting real-time ordering, then cover membership handoff without node failure. Check intermediate reads and delete semantics, not only final state. Retain the possibility of application for `MayHaveApplied` operations rather than treating timeouts as non-execution. Validate OwnerOnly failover separately under its permitted rollback contract instead of requiring strict linearizability across failover.

   **Status: complete.** `tests/process_cluster/history.rs` runs four independent clients issuing concurrent PUT/GET/DELETE against a set of keys, recording arguments, invocation/completion times, returned values, versions, and epochs. The checker orders mutations by the version their owner assigned, verifies every real-time constraint (a mutation that completed before another started has the earlier version), and finds a legal serialization point for every read: mutations before it must have started before the read completed, mutations after it must have completed after the read started, and the value and version must match that point. The two tests cover one owner epoch and a 3→4 membership handoff without a node failure; concurrent histories of 160 operations across two keys passed, including reads that observed an intermediate delete. The `MayHaveApplied` two-branch model is implemented in the `fault` mode; the OwnerOnly failover rollback contract is validated separately in [fault-under-load.md](fault-under-load.md#owner-丢失3-次同一配置), not by strict linearizability across failover.

   **Limitation:** the checker is for a single register; both tests require successful operations and do not mix timeouts or `MayHaveApplied` into the concurrent history.

Execution order (completed): add request-outcome records under load first, and complete the small correctness checks in 7 and 8; then run 4 and 5. Give each run a separate result directory and retain the effective configuration, source and binary identity, events, process logs, topology/replica status, and partial results on failure. Before citing a completed experiment in this checklist, record its configuration, source revision, key results, and limitations in a Git-tracked document and link that document; runs available only in local `results/` are not shared baselines.
