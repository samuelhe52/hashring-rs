# 性能与高可用压力测试 TODO

## 中文

主测试配置采用当前默认值：RF=3、`OwnerOnly`、`minimum_admitted_copies=1`、`minimum_healthy_followers=0`。每个范围期望有两个异步跟随副本，但 `OwnerOnly` 写入不等待跟随副本 ACK。保持默认 30 秒逻辑操作期限；延长期限的运行只能作为单独的诊断结果。

干净源码版本 `89d23fa` 已有九次固定配置的 100 万键性能运行全部完成，以及二十次 2 万键 `FirstSuccessor` 迁移运行全部完成。因此下面的工作重点是负载形态、故障期间的行为和未覆盖的规模，不重复刷相同配置的基线。性能比较在相同机器条件下至少重复三次，保留每次的原始结果；失败运行也保留已完成阶段和错误证据。

1. [ ] **稳态容量、延迟与操作期限。** 扩展实验 crate 的工作负载，在 10 节点、RF=3、`OwnerOnly` 下分别测试并发度 1/16/64/128、128 B/4 KiB 值，以及持续的 PUT、GET、PUT/GET/DELETE 混合负载。先分析现有 100 万键运行中 `OwnerOnly` 写入速率波动，再决定是否需要更多相同配置的重复运行。记录负载进行时的吞吐量和 p50/p95/p99 延迟、按原因分类的重试与期限结果、各节点 CPU/RSS、跟随副本延迟；逐阶段核验写入、读取和删除结果。不要用批量写入结束后的少量串行 PUT 延迟代表负载中的尾延迟。

2. [ ] **复制积压与恢复。** 优先做可控的跟随副本减速或短时断链实验：在持续写入后停止负载，按时间采样每条流的队列深度、容量拒绝、复制延迟、内存和恢复耗时。核验容量耗尽时在应用变更前返回可重试错误、积压保持有界、链路恢复后按序追平且数据正确。先手工注入故障；只有注入机制可复用时才加入实验 crate。RF=1 与 RF=3 的匹配基线仅在需要量化复制开销时执行，不扩展成完整配置矩阵。

3. [ ] **持续流量下的成员迁移。** 以默认 `OwnerOnly` 运行 9→10→9 节点迁移，从 10 万键和持续混合操作开始；稳定后再提高到 100 万键。覆盖快照复制、切换和 RF 修复，并在每个阶段核验成功写入的最终值与成功删除的缺失状态。记录迁移阶段耗时、原始节点 RPC 的临时阻塞、客户端重试原因、操作期限与恢复到完整 RF 的时间。现有 2 万键 `FirstSuccessor` 重复运行不能替代这个场景。

4. [ ] **负载中的跟随副本丢失。** 使用至少四个初始节点，预先选定“所有者存活、故障节点是跟随副本”的键，再杀死该跟随节点；持续执行默认 `OwnerOnly` 写入和读取。核验这些范围仍可写、RF 下降有状态可查、其他范围不被误阻塞，并在替换节点加入后完成修复与数据核验。现有 availability 模式固定为三个初始节点加一个替换节点；此场景需要新实验分支或手工集群。

5. [ ] **负载中的所有者丢失。** 保留现有低负载 owner-kill 结果作为基线，不再重复同一个采样配置。在故障前、故障中和修复后持续执行混合操作，换多个 hash seed 与故障所有者；记录每个请求的 ID、键、意图、时间、结果和拓扑 epoch，以便恢复后核对不确定写入。测量受影响范围的不可用区间、发布新拓扑、读写恢复、新所有者分布及 RF 修复。将 `OwnerOnly` 允许的近期已 ACK 写入丢失单独统计；错误数据、旧所有者成功写入或无法恢复仍判为失败。

6. [ ] **协调器重启与连续节点更替。** 在上述单次故障实验稳定后，先增加一次“带负载且正在修复”的协调器重启，核验租约自我隔离、拓扑恢复以及没有冲突所有者。随后运行 30–60 分钟的连续更替：每次只让一个节点故障，等上一次修复完成后再进行下一次。逐轮记录不可用时间、RF 恢复、重试与 CPU/RSS/复制延迟漂移。协调器高可用不在当前契约内，因此不要把重启期间完全无中断作为通过条件。

建议执行顺序：先补齐负载中的延迟、重试和请求结果记录；再做 4、5、3；随后做 2；最后做 6 的长时间运行。每次运行使用独立结果目录，并保存实际配置、源码与二进制标识、事件日志、进程日志、拓扑/副本状态及失败时的部分结果。

## English

Use the current defaults for the main profile: RF=3, `OwnerOnly`, `minimum_admitted_copies=1`, and `minimum_healthy_followers=0`. Each range desires two asynchronous followers, but an `OwnerOnly` write waits for neither follower's ACK. Keep the default 30-second logical operation deadline; runs with a longer deadline are separate diagnostics.

On clean source revision `89d23fa`, all nine fixed-profile one-million-key performance runs and all twenty 20k-key `FirstSuccessor` migration runs completed. The work below targets different load shapes, behavior during faults, and larger migrations instead of repeating those same baselines. Repeat performance comparisons at least three times under comparable machine conditions and retain every raw run, including partial evidence from failures.

1. [ ] **Steady-state capacity, latency, and deadlines.** Extend the experiment crate workload to run sustained PUT, GET, and mixed PUT/GET/DELETE traffic on 10 nodes with RF=3 and `OwnerOnly`. Sweep concurrency 1/16/64/128 and 128 B/4 KiB values. First investigate the write-rate variation in the existing one-million-key OwnerOnly runs before scheduling more identical repeats. Record throughput and p50/p95/p99 latency during load, retries by cause, deadline outcomes, per-node CPU/RSS, and follower lag; verify writes, reads, and deletes after each phase. Do not treat the few serial PUTs sampled after bulk loading as loaded tail latency.

2. [ ] **Replication backlog and recovery.** Prioritize a controlled slow-follower or short link-loss run: write continuously, stop the load, and sample each stream's queue depth, capacity rejections, replication lag, memory, and drain time. Verify retryable rejection before mutation application when capacity is exhausted, bounded backlog, ordered catch-up, and correct data after recovery. Inject the fault manually first; add it to the experiment crate only if the mechanism is reusable. Run one matched RF=1 versus RF=3 baseline only when quantifying replication overhead is needed.

3. [ ] **Membership migration under continuous traffic.** Run 9→10→9 nodes with default `OwnerOnly`, starting at 100k keys and continuous mixed operations; increase toward one million keys after the smaller run is stable. Cover snapshot copy, cutover, and RF repair. After each stage, verify final values for successful writes and absence for successful deletes. Record phase durations, temporary blocks seen through raw node RPCs, client retry causes, operation deadlines, and time to full RF. The existing repeated 20k-key `FirstSuccessor` runs do not cover this profile.

4. [ ] **Follower loss under load.** Start with at least four nodes and select keys whose owner stays alive while a follower node is killed. Continue default `OwnerOnly` writes and reads. Verify that those ranges remain writable, the reduced RF is reported, unrelated ranges are not blocked, and a replacement catches up with correct data. The current availability mode fixes the layout at three initial nodes plus one replacement, so use a new experiment scenario or a manual cluster.

5. [ ] **Owner loss under load.** Keep the existing low-load owner-kill results as the baseline rather than repeating the same sample. Run mixed traffic before, during, and after failure, varying the hash seed and failed owner. Retain a request ledger with ID, key, intended operation, time, result, and topology epoch to reconcile uncertain writes after recovery. Measure affected-range unavailability, topology publication, read/write recovery, new-owner distribution, and RF repair. Count permitted recent acknowledged loss under `OwnerOnly` separately; wrong data, successful writes from a fenced owner, and failure to recover remain failures.

6. [ ] **Coordinator restart and sequential churn.** Once the single-fault scenarios are stable, restart the coordinator once during loaded repair and verify lease self-fencing, topology recovery, and absence of conflicting owners. Then run 30–60 minutes of sequential node failures and replacements, waiting for each repair to finish before the next fault. Record per-cycle unavailability, RF recovery, retries, and drift in CPU/RSS and replication lag. Coordinator HA is outside the current contract, so uninterrupted service throughout a coordinator restart is not a pass condition.

Execution order: add in-load latency, retry, and request-outcome records first; run 4, 5, and 3 next; then run 2; leave the long run in 6 for last. Give each run a separate result directory and retain the effective configuration, source and binary identity, events, process logs, topology/replica status, and partial results on failure.
