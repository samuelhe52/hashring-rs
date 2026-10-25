# 负载中的节点故障、重试回执与同键历史

本文记录 `docs/TODO.md` 第 4、5、7、8 项的实现、验证方法和结果。原始结果仍在本地 `results/`（或本轮的 `/tmp/opencode/`），不纳入 Git；这里只保留配置、源码标识、数值和限制。

## 源码与运行条件

- 代码基线：`563ffd0`（分支 `remainder`）。运行时的代码与该提交一致；工作树里只有本文档的未提交修改和未跟踪的审查文件（`manifest.json` 的 `git_status_porcelain` 记录了实际状态），因此这些仍不是干净源码上的正式基线。
- 主机：当前开发机，24 逻辑 CPU；`--worker-threads 2`，驱动与每个数据节点共享主机。
- 编译：debug profile（`cargo run --quiet -- experiment ...`）。绝对吞吐只用于同轮内比较。
- 公共配置：5 节点（4 个初始成员 + 1 个替换节点）、RF=3、`OwnerOnly`、128 B 值、seed 1、128 个虚拟节点、30 s 操作期限、120 s 迁移期限、`GET:PUT:DELETE=70:20:10`、16 个负载 worker。
- 故障前负载 5 s，满 RF 恢复后继续负载 5 s。故障后的替换与 RF 修复都在负载持续期间完成。

## 实现

### `fault` 实验模式

新增 `experiment --mode fault`，两个场景：

- `--fault-scenario follower`：选出一个节点，只使用它作为跟随副本（owner 不是它）的键。杀死该节点后，这些键的 owner 仍须是原来的节点；负载继续写入和读取。
- `--fault-scenario owner`：选出被一个节点拥有的键，杀死该 owner，负载跨自动选主、替换加入和 RF 修复持续运行。

每个请求写入 `requests.jsonl`，字段包括逻辑请求号、worker、键、操作、值（hex）、调用/完成 Unix 毫秒与耗时、调用前后的拓扑 epoch、结果、错误码、`KnownNotApplied/MayHaveApplied`、返回版本、RPC 重试次数和按原因的重试计数。`summary.json` 的 `fault_rounds` 记录：

- 受害者、受害 epoch、选中键数、涉及的 range 数；
- 故障前所选 range 的 `current_rf`、`live_rf`、`writable`、`under_replicated`、跟随副本的 `stream_head/stream_cursor/last_ack_sequence/lag`；
- 从 kill 到发布、到首次成功读/写/删、从替换开始到满 RF、从 kill 到满 RF；
- kill 之后完成的读、写、删次数，以及 kill 后第一次/最后一次失败的毫秒数（没找到记录的读也算成功的读：这些计数说明读路径仍然工作，不说明数据还在）；
- 故障期间和发布后的 `live_rf`、under-replicated、not-writable range 数，以及不可写的采样次数与原因（`write_block_reason_samples_*` 计的是“采样 × range”的对数，不是不同 range 的个数）；
- 请求量、错误、重试、`MayHaveApplied`、回退、无法判定和无法解释的读、被丢弃的已确认状态数；
- 新 owner 分布和最终校验结果。

每一轮的检查在轮结束后统一收集：一轮失败即结束运行，但该轮和之前各轮的记录仍写进 `summary.json` 的 `fault_rounds`，失败原因写进 `observations.fault_failures`（同时出现在 `error` 文本里），失败的运行因此带着它看到的东西，机器读 `summary.json` 不必解析错误文本。运行级字段：`fault_acknowledged_window` 是本次运行使用的确认窗口大小；`fault_rounds_requested` / `fault_rounds_completed` 是请求的轮数和实际完成的轮数（失败时后者更小，`fault_rounds` 的长度与它一致）；`fault_dropped_acknowledged_states` 是各轮丢弃数之和，每轮的 `dropped_acknowledged_states` 只算该轮，逐轮相加不会重复计数。

所有时长都从 kill 起算，和 `availability` 模式的同名字段口径一致；负载在故障前跑的那段窗口不计入其中。早期实现把“负载开始”当原点，`首次读/写` 实际报的是 pre-window 的长度，本文的结果已按新口径重跑。

`ResourceExhausted` 的容量背压分成两个字段，不再混算：`capacity_backpressure_errors` 是“以容量错误结束”的请求数，`capacity_backpressure_retries` 是客户端因容量而重试的次数。同一次请求不会再被两个字段各记一次。

### 请求账本

`requests.jsonl` 每个请求一行，约 580 B：10 万请求约 58 MB，100 万请求约 580 MB。行按**完成顺序**写入，而逻辑请求号按**调用顺序**分配，所以文件不是按请求号有序的；需要按调用顺序处理时按 `invoked_unix_ms` 排序即可。账本没有上限，长时间运行前要先估算磁盘占用。

### 历史模型

每个键维护一个单寄存器模型：

- 成功 PUT/DELETE 把可能状态收窄为唯一值+版本（DELETE 也返回 tombstone 版本，见 `DeleteResponse.version`）；
- `MayHaveApplied` 的 PUT/DELETE 保留“旧状态”和“新值/缺失”两个分支；
- 故障后的读如果命中唯一状态则正常；如果命中历史中确认过的旧状态，记为一次回退，并按 `older_value_reappeared`、`acknowledged_delete_lost`、`acknowledged_value_lost` 分类；
- 历史中不可能出现的值记为 `impossible_reads`，运行失败；
- `OwnerOnly` 的 owner 故障允许回退，回退单独计数；follower 故障不允许回退，因为 owner 一直存活，出现回退即失败。

已确认历史（“这个键曾经确认过哪些状态”）按值和版本的摘要保存。每个键保留多少条由 `--fault-acknowledged-window` 决定（默认 4096 条），超出时丢弃最旧的并计数。明细字段在该轮的 `dropped_acknowledged_states`，运行合计在 `observations.fault_dropped_acknowledged_states`；一个键的模型跨轮保留，所以每轮报的是它自己丢弃的条数（轮开始时快照、轮末取差值），逐轮相加等于合计。

内存上限是 `键数 × 窗口 × 每条约 90 B`：每条状态是值和版本的两个 blake3 摘要（`Option<[u8;32]>`，约 66 B）加集合索引开销。默认 owner 场景 208 个键、窗口 4096 若全部装满约 75 MB，而默认运行每键只装到两三百条，实际约 5 MB。按需调大窗口时要按这个式子估算内存。写路径用集合判重，确认一个新状态是 O(1)；只有重新确认窗口里已有的状态时才线性找位置并移到末尾（这种情形很少）。读路径只在读不匹配当前状态时线性扫描一次窗口。原因与取舍见 [fault-window-dedup.md](optimizations/fault-window-dedup.md)。

窗口必须覆盖负载期间每个键的写入量。一条已确认状态一旦被丢弃，回退到它就和“历史里从来没有过的值”无法区分，读到它会让该轮不通过。默认值对默认运行（平均每键约 200 次写）有二十倍余量；更长、更快或键更少的运行要相应调大，经验值是预期每键写入数的 4 倍以上。

丢弃是有代价的：一个读若既不匹配当前状态，也不匹配保留下来的任何已确认状态，就无法再区分“回退到被丢弃的旧状态”和“历史里从来没有过的值”。这种读记为**无法判定**（`undecidable_reads`，样本在 `undecidable_samples`），单独统计，不计入通过：

- follower 场景中 owner 一直存活，任何不匹配当前状态的读都是不允许的回退；
- owner 场景中 `OwnerOnly` 允许回退，但被截断的历史无法把回退和“从来没有过的值”分开。

两种场景都判该轮**不通过**：证据不足的读不是被核验过的读，运行不能把它算作通过（TODO 第 5 项）。失败信息里写明窗口被截断、“这些键在本次运行中累计丢弃 N 条”（不是这些读所属单个键的条数，也不是本轮丢弃的条数）和无法判定的样本，以免读成“跟随副本丢失本身造成的异常”。相应地，“0 次无法解释的读”这一结论只在被丢弃状态数为 0 时才是完整结论。

### 操作的分布

`GET:PUT:DELETE` 的配比按步长 walk 分配给各 worker，步长与总槽位互素（从 37 起找第一个互素值）。固定的 37 只在 `gcd(37, total) == 1` 时才互素：`--mix 20:27:27`（total=74）下每个 worker 只能取到两种操作。现在步长按 total 求出，每个 worker 都会取到三种操作。

### 回执重试测试

`tests/process_cluster/retry.rs` 新增两个真实进程测试。它们用 `put_with_mutation_id` / `delete_with_mutation_id` 固定 mutation ID，第一次成功响应被驱动主动丢弃（等价于响应丢失，mutation 已应用），随后：

1. 完成 3→4 成员迁移，或 `FirstSuccessor` 下杀死 owner 并提升原第一跟随副本；
2. 用同一 mutation ID 和 payload 重试，断言返回原版本；
3. 写入同键更新值，再重试旧请求，断言新值仍在；
4. 用同一 ID 但不同 payload 重试，断言得到 `MutationIdConflict`；
5. PUT 与 DELETE 都覆盖；
6. 测试把 `retry-trace.jsonl` 和同键并发历史写到 `target/test-artifacts/<测试名>-<pid>/`，可在测试结束后检查；`target/` 不纳入 Git。

客户端的两个新 API 在本地校验 mutation ID：为空或超过 `MAX_MUTATION_ID_BYTES`（128 B）的 ID 直接以 `InvalidArgument` 失败，不绕到节点再被拒绝；ID 的唯一性要求（回执保留期内不得被别的操作复用）写在 API 文档里。

`FirstSuccessor` 场景在注入 owner 故障前等待 follower 的 `stream_head == stream_cursor`，即 mutation 及其回执已经到达将被提升的副本，从而把去重验证与“允许的异步尾部丢失”分开。

### 同键并发历史测试

`tests/process_cluster/history.rs` 用 4 个独立客户端对同一组键并发执行 PUT/GET/DELETE，每个操作记录调用/完成时间、参数、返回值、版本和拓扑 epoch。检查器按 owner 分配的版本对 mutation 排序，然后要求：

- 任一 mutation 完成早于另一个开始，则版本顺序一致；
- 任一读都能找到一个合法串行化点：它之前的 mutation 都在读完成前开始，它之后的 mutation 都在读开始后才完成；
- 返回值和版本必须与串行化点的状态一致。

两个测试分别覆盖单一 owner epoch，以及无故障的 3→4 成员交接（交接前和交接后各一轮并发历史，中间等待满 RF）。

## 结果

以下所有时长都从 kill 起算（`read/write/delete_recovery_seconds`、`first/last_failure_after_kill_ms`），与 `availability` 模式的同名段口径一致；故障前的负载窗口不计入其中。

### Follower 丢失（3 次，同一配置）

| 运行 | 受害节点 | 选中键 | range | kill→发布 s | 首次读/写/删 s | kill→满 RF s | 请求 | 应用错误 | 回退 | 不可解释读 | 无法判定 | 丢弃状态 | 容量背压（错误/重试） |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | node-1 | 576 | 180 | 5.43 | 0.002 / 0.003 / 0.004 | 15.72 | 132,123 | 0 | 0 | 0 | 0 | 0 | 0 / 510 |
| 2 | node-1 | 576 | 180 | 5.41 | 0.002 / 0.002 / 0.001 | 15.88 | 135,743 | 0 | 0 | 0 | 0 | 0 | 0 / 506 |
| 3 | node-1 | 576 | 180 | 5.45 | 0.002 / 0.003 / 0.006 | 16.50 | 134,471 | 0 | 0 | 0 | 0 | 0 | 0 / 508 |

三组都满足：

- 发布后所选 180 个 range 的 owner 全部不变；
- 跟随副本被杀死后，读写没有中断：第 1 次运行在 kill 之后仍完成 62,872 次读、17,962 次写和 8,981 次删，应用错误为 0，说明 `OwnerOnly` + `minimum_admitted_copies=1` 没有因为副本数下降而拒绝这些写；
- 客户端仍会因 `RangeBusy`（切换栅栏）和 `ResourceExhausted`（复制容量背压）内部重试：三次分别有 510/506/508 次 `resource_exhausted` 重试，没有以容量错误结束的请求；两类计数分开，不再混算；
- 故障期间（kill 到发布）采样到的状态：180 个 range 全部 under-replicated，`live_rf` 最低 0；有采样把 range 标为不可写，原因是 `owner lease unavailable`（协调器在故障窗口内的租约视图尚未刷新），但 owner 仍接受了全部写入，没有请求因此失败；发布后 `live_rf` 最低 1/3、180 个 range under-replicated、没有不可写 range；替换并满 RF 后全部恢复；
- 回退、无法判定和无法解释的读均为 0，被丢弃的已确认状态为 0（默认 4096 条窗口远大于每个键的写入量）。

### Owner 丢失（3 次，同一配置）

| 运行 | 受害节点 | 选中键 | range | kill→发布 s | 首次读/写/删 s | kill→满 RF s | 请求 | 回退 | 不可解释读 | 无法判定 | 丢弃状态 | `MayHaveApplied` |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | node-1 | 208 | 84 | 5.36 | 5.556 / 5.556 / 5.556 | 15.74 | 131,912 | 4 | 0 | 0 | 0 | 5 |
| 2 | node-1 | 208 | 84 | 5.63 | 5.796 / 5.798 / 5.797 | 16.22 | 133,375 | 5 | 0 | 0 | 0 | 4 |
| 3 | node-1 | 208 | 84 | 5.57 | 5.742 / 5.742 / 5.745 | 15.83 | 131,322 | 4 | 0 | 0 | 0 | 4 |

- 读和写都在 kill 后约 5.6–5.8 s 恢复：kill 后约 5.4–5.6 s 发布新拓扑，客户端再完成重路由并等到新 owner 可写。
- 三次记录了 `OwnerOnly` 下可回退的已确认写入（4/5/4 个键），没有历史无法解释的值，也没有无法判定的读；回退分类为已确认值丢失（3、3、0 次）和旧值重现（1、2、4 次）。
- 应用层错误只有少量 `rpc:Unknown`（三次各 16 次，来自被杀死进程上的在途 RPC）和第 3 次额外的 2 个 `rpc:Cancelled`；kill 之后失败的请求为 14/11/10 次，第一次失败距 kill 2/2/1 ms，最后一次 4/3/2 ms。
- 发布后所选 range 仍有 69/84 under-replicated，随后通过替换和 RF 修复在 15.7–16.2 s（从 kill 起算）内恢复满 RF；三次运行的负载长度接近（13.1–13.3 万请求、26 s 左右），满 RF 时间的差异与运行间波动同级。
- 新 owner 分布在幸存节点和替换节点之间，`new_owner_counts` 随运行记录。

### 多次 owner 故障与不同 seed

`--seed 7 --fault-rounds 2 --nodes 6 --keys 512` 连续杀死两个 owner：

| 轮 | 受害节点 | 选中键 | range | kill→发布 s | 首次读/写 s | 回退 | 回退分类 | 丢弃状态 | kill→满 RF s |
|---|---|---|---|---|---|---|---|---|---|
| 1 | node-1 | 141 | 64 | 5.74 | 5.882 / 5.883 | 8 | 已确认删除丢失 4、已确认值丢失 2、旧值重现 2 | 0 | 16.24 |
| 2 | node-3 | 121 | 65 | 5.16 | 5.329 / 5.333 | 2 | 已确认值丢失 1、旧值重现 1 | 0 | 15.74 |

两轮都没有不可解释读和无法判定的读，且第二轮在第一次替换完成后的新拓扑上选择新的 owner，说明多轮扫描和按轮记录可用。每轮的 `dropped_acknowledged_states` 只算该轮（轮开始时快照、轮末取差值），`observations.fault_dropped_acknowledged_states` 是各轮之和，两者都是 0：第二轮 121 个键共约 2.3 万次变更（平均每键约 190 次），默认 4096 条窗口覆盖了最热的键。上一版 256 条窗口的同类运行（负载更长，修复等待也更长）曾累计丢弃 2,267 条；把窗口压到 64 再跑同样的两轮，两轮分别丢弃 15,745 和 17,366 条、合计 33,111 条，逐轮数字因此是可加的。这正是把逐轮数字改成差值、并补运行合计的原因。

### 回执重试与同键历史

以下测试均通过：

```
cargo nextest run --test process_cluster retry::
cargo nextest run --test process_cluster history::
```

- `response_loss_retry_after_migration_reuses_result_and_keeps_newer_value`
- `response_loss_retry_after_owner_promotion_reuses_result_and_keeps_newer_value`
- `concurrent_same_key_histories_are_linearizable_within_one_owner_epoch`
- `concurrent_same_key_histories_are_linearizable_across_a_membership_handoff`

同键并发测试共 4 客户端 × 40 次操作、在两个键之间轮转（约 80 次/键），迁移场景在交接前和交接后各一轮；检查器没有发现违反实时顺序的 mutation 或无法解释的读取。

## 复现

Follower：

```
cargo run -- experiment \
  --mode fault --fault-scenario follower \
  --nodes 5 --keys 1000 --concurrency 16 \
  --fault-pre-seconds 5 --fault-post-seconds 5 \
  --operation-timeout-ms 30000 --migration-timeout-ms 120000 \
  --worker-threads 2 --output results/fault-follower
```

Owner（把 `follower` 换成 `owner`）：

```
cargo run -- experiment \
  --mode fault --fault-scenario owner \
  --nodes 5 --keys 1000 --concurrency 16 \
  --fault-pre-seconds 5 --fault-post-seconds 5 \
  --operation-timeout-ms 30000 --migration-timeout-ms 120000 \
  --worker-threads 2 --output results/fault-owner
```

多轮 owner 故障：

```
cargo run -- experiment \
  --mode fault --fault-scenario owner --seed 7 --fault-rounds 2 \
  --nodes 6 --keys 512 --concurrency 8 \
  --fault-pre-seconds 2 --fault-post-seconds 2 \
  --operation-timeout-ms 10000 --migration-timeout-ms 120000 \
  --worker-threads 2 --output results/fault-owner-multi
```

## 限制

- 这些运行来自 debug 构建，代码基线为 `563ffd0`，不能当作干净源码上的正式性能基线；正式复测应使用 `--require-clean-source`、release 构建和空闲主机。
- `fault` 模式的容量背压分成两个计数：以容量错误结束的请求数，和客户端因容量而重试的次数；一次跟随副本丢失仍可能触发复制容量背压，本文按 TODO 要求单独记录，不要求故障期间零阻塞。
- follower 场景中，被杀死节点对其他 range 仍是 owner；这些键不在本场景的判定范围内，按“附带 owner 故障”单独记录并重新同步模型，不作为 follower 丢失的通过/失败条件。
- 协调器在故障窗口内把所选 range 标为不可写（`owner lease unavailable`）时，owner 仍会接受写入；该状态是协调器视图，与实际是否拒绝写入不是一回事，本文按状态原样记录。
- 每个键的已确认历史有上限，由 `--fault-acknowledged-window` 给出（默认 4096 条），超出后丢弃最旧的并计数；被重新确认的状态会移到窗口末尾，因此窗口里是最近确认过的状态。窗口太小会让可解释的回退变成无法判定：一个既不匹配当前状态也不匹配保留历史的读只能记为无法判定，两个场景都判该轮不通过（该读没有被核验过，运行不能算它通过）。默认长度的一次 owner 运行平均每键约 200 次写，默认窗口有二十倍余量；个别键的写入可能高于平均值，更长的运行按实测的 `dropped_acknowledged_states` 调大窗口，否则长跑中“0 次不可解释读”这一结论只在被丢弃状态数为 0 时才完整。
- `requests.jsonl` 约 580 B/请求且没有上限（10 万请求约 58 MB），按完成顺序写入而不是按请求号排序；长时间运行前要先估算磁盘占用。
- 回执重试测试把“响应丢失”实现为：mutation 已成功应用，驱动丢弃成功响应后换 epoch 重试。它验证了服务端回执、迁移/提升携带回执和客户端复用 mutation ID，但没有在传输层真实丢弃一个 TCP/HTTP2 响应；客户端内部的每一次 RPC attempt 也没有逐条持久化，`retry-trace.jsonl` 只关联逻辑请求、mutation ID、epoch 和最终结果。
- 同键历史检查器针对单寄存器，按版本排序 mutation；它覆盖单 owner epoch 和“无故障迁移前后”的交接，不覆盖 owner 故障期间的严格线性一致性（那部分按 `OwnerOnly` 允许回退的契约单独处理）。
- 故障实验使用 seed 1 的三次重复和 seed 7 的一次两轮运行；更多 seed/受害节点组合仍可继续扩展。
