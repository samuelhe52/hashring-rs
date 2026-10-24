# 扩缩容期间读写实验（方案 v4）

> 状态：`scaling` 模式已实现，正式矩阵已在干净源码 `b0ce975` 上完成（§0.1）。原始 `results/` 不纳入 Git。
> 本文整合了三轮评审意见，只保留中高价值项。

## 0. 结论先行

- 主线收窄为 **同一个集群里 9→10→9 往返一次**（S1 扩容接 S2 缩容），对齐 `docs/TODO.md` 第 3 项。
- **S3 替换、S4 多节点连续迁移往后排**；先保证主线可解释、可重复。
- 增加一组**相同配置、不做迁移的对照运行**，用来区分正常波动与迁移影响。
- 主性能组必须显式设置 `pre_publish_delay_ms = 0`，否则会人为拉长停写窗口、污染写尾延迟。
- “持续可用”的结论不能由少量零失败样本推出；硬性通过标准只放正确性，可用性和延迟作为可观测结果报告。

## 0.1 实现状态与正式结果

方案条目已全部实现。正式矩阵来自干净源码 `b0ce975`：每次运行都带 `--require-clean-source`，manifest 记录的 build 与 runtime 提交、源码摘要一致。原始结果在本地 `results/scaling-refresh-<时间戳>/`，不纳入 Git。主机是 24 逻辑 CPU、32 GB 的共享桌面机；这一轮运行期间桌面进程较忙（`loadavg` 约 5，Firefox 等占 CPU），所以**绝对吞吐低于更早 `d0b160f` 那一轮**，两组数字不能直接比较绝对值，只能看同一轮内的相对关系。

这一轮的计时口径有三处修正，因此 `replay`、`满 RF s`、`停写窗口 ms` 三列与 `d0b160f` 的记录不可比：`changelog_replay_ms` 只算回放本身（不再把 digest 往返与 commit 算进去），`full_rf_seconds` 从 `Complete` 起算（含等被移除节点退出），停写窗口以每个 transition 开始时的计数为基线（第二个 transition 不再把第一个的拒绝算成自己的）。

> **上一轮之后的计时口径修正**：`publish_ms` 现在覆盖发布工作一直到承载它的那次持久化写之前，写本身不计入；policy 路径也测量 `pause_writes_ms`、`verify_ms`、`publish_ms` 与 `activation_ms`；计时与阶段变更在同一次持久化写中落库。下表是修正之前的口径，只作为量级参考。

### 已实现

- 场景：`round-trip` / `grow` / `shrink` / `replace`（S3 等成员数替换）/ `churn`（S4 多节点连续迁移）/ `control`（S0 无迁移对照）。
- 开环复核组：`--scaling-load-mode open --scaling-open-rate <rate>`，固定总速率，GET:PUT:DELETE = 80:12:5:3（读 80%，rewrite 12%、新写 5%、删除 3%）；超过 `--max-in-flight` 的请求丢弃并计 `late`。每秒采样与汇总都记录 `issued`、`late`、`in_flight`。
- 四类 cohort、按速率发放、局部 drain（只等本 transition 的写，读继续）、S1 后中间校验、最终全量校验、读己之写/删、发布后复读、按阶段与每秒指标、按原因的重试计数。
- coordinator 工作步骤计时：`snapshot_copy_ms`、`changelog_replay_ms`、`pause_writes_ms`、`verify_ms`、`publish_ms`、`activation_ms`、`merge_proof_ms`、`catch_up_ms`，随 change 快照返回并写入 `work_steps_ms`。
- **客户端观测的停写窗口**：变更期间每 5 ms 采样客户端的 `RangeBusy` 计数与成功写计数，写入 `<label>_transition.stop_window`：`first_range_busy_ms`、`last_range_busy_ms`、`first_write_ok_ms`、`observed_ms`、`poll_ms`。停写时长以客户端观测为准，不以 stage 边界为准。
- **五个时间点**：`published_ms`、`complete_ms`、`full_rf_seconds`（`Complete` 之后到目标 epoch 满 RF），配合 `events.jsonl` 的 `migration_phase_changed` 与 `work_steps_ms` 可还原开始、发布、激活、`Complete`、满 RF。
- 100 万键规模：`--keys 1000000` 可直接运行，池、预载和校验按该规模计算。

### 容量基线（开环组 60% 的依据）

开环组的速率来自同配置实测容量，不是估值。容量用 `latency` 模式在开环组的同一配置下测得：10 节点、RF=3、`OwnerOnly`、1M 键、128 B、GET:PUT:DELETE = 80:15:5、`zipf:0.99`、in-flight 4096、`--max-dedup-bytes 268435456`：

| 提供速率 ops/s | 实际速率 | late | 重试 | GET p50 | GET p99 | CPU 总和 |
|---|---|---|---|---|---|---|
| 40,000 | 40,000 | 0 | 0 | 66 µs | 143 µs | 240% |
| 60,000 | 60,000 | 0 | 0 | 63 µs | 222 µs | 340% |
| 80,000 | 80,000 | 0 | 0 | 72 µs | 698 µs | 441% |
| 90,000 | 90,000 | 0 | 0 | 86 µs | 1.0 ms | 498% |
| 100,000 | 100,000 | 0 | 946 | 164 µs | 2.4 ms | 560% |

100k 是最后一条零丢弃档位；同一配置的试探档显示 110k 起饱和（实际 92.6k、late 347k、GET p50 39.7 ms）。因此取 **C = 100,000 ops/s**，开环组目标总量 = 0.6×C = 60,000 ops/s。开环模式每成功写一次就做一次读己之写探针（写占 offered 的 20%），所以 offered rate 取 **50,000**，实际总请求约 60,000。

回执预算是这组配置的一部分：1M 键预载后每节点已有约 95 MB 回执，默认 128 MB 预算下开环写速率会被回执背压拉低（同一档位实测只跑到 24.6k）；矩阵脚本的 open 组因此默认 `--max-dedup-bytes 268435456`，容量基线用同样的值。重试 946 是客户端内部重试，应用层错误为 0。

### 主组：100k 键 9→10→9（3 次，中位数）

| 阶段 | 实际路径 | complete ms | published ms | 满 RF s | snapshot | replay | pause | verify | publish | activation | RangeBusy | 停写窗口 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| grow | 复制 | 2772 | 2090 | 4.45 | 1021 | 2144 | 11 | 1026 | 16 | 667 | 28 | 1098 |
| shrink | `direct_merge` | 163 | 89 | 6.58 | 0 | 0 | 9 | 48 | 16 | 61 | 16 | 101 |

- 除 `RangeBusy` 与窗口外，单位为毫秒；`replay` 是各 range 之和，可大于所在阶段。
- shrink 的 `merge_proof_ms` = 6、`catch_up_ms` = 25。
- 三次运行各约 4.2–4.5M 次操作、49.8–50.2 s，`late=0`；grow/shrink 期间聚合速率 88.7–95.4k / 98.8–101.8k ops/s，同一轮对照窗口 102.2–104.5k ops/s，所以 grow 比对照低约 7–15%，shrink 低约 0–5%。
- 停写窗口（客户端观测）：grow 在 1060 ms 首次出现 `RangeBusy`、2096 ms 最后一次、2158 ms 首个成功写；shrink 为 44 / 95 / 139 ms。grow 的栅栏在复制与回放之后才加上（1060 ms 约等于 `snapshot_copy_ms` 之后进入 `PausingWrites`），一直保持到发布（2096 ms 对 `published_ms=2090`），所以窗口主体是 `Verifying`（1026 ms）+ `publish`（16 ms），不是整个 change；shrink 走 direct merge，栅栏在 change 开始后约 44 ms 才第一次拒绝掉写，窗口约 101 ms，不是“一开始就停写”。
- 分层：grow 覆盖 `new_target`、`replicas_changed`、`unchanged`；shrink 覆盖 `removed_source`、`replicas_changed`、`unchanged`。S1 cohort 在 S2 开始时校验（`deferred_to_next_transition`），S2 校验通过。

GET 延迟按请求**发起时**所在阶段（操作数为单次运行的平均值，百分位取 3 次中位数）：

| 阶段 | 操作数 | p50 | p99 |
|---|---|---|---|
| `pre`（变更前） | 1,053,800 | 600 µs | 1.14 ms |
| `copying_snapshot` | 91,802 | 683 µs | 1.69 ms |
| `replaying_changelog` | 773 | 752 µs | 1.42 ms |
| `pausing_writes` | 3,617 | 691 µs | 1.38 ms |
| `verifying` | 94,265 | 668 µs | 1.61 ms |
| `published → complete` | 57,033 | 321 µs | 1.02 ms |
| `post`（含 RF 修复） | 3,013,416 | 629 µs | 1.56 ms |

被栅栏拒绝的写：`pausing_writes` 中 `put_rewrite` 中位数 4 次（p50 109 ms、p99 1.21 s），`verifying` 中 2 次（p99 1.26 s）——写一直阻塞到发布。读在这些阶段基本不受影响。

### S0 对照（3 次，中位数）

| 用时 s | 操作数 | late | 聚合 ops/s | GET p50 | GET p99 |
|---|---|---|---|---|---|
| 26.1 | 2,094,280 | 0 | 80,104 | 710 µs | 1.43 ms |

对照组的节点数（10）、线程预算与主组一致。主组 grow 的聚合速率比对照低约 7–15%（88.7–95.4k 对 102.2–104.5k 的测量窗口），shrink 低约 0–5%（98.8–101.8k）；两次 change 的停写窗口分别约 1.1 s 和 0.10 s，而对照没有 `RangeBusy`。

### 诊断、策略对照、S3、S4（各 1 次）

| 组 | 变更 | complete ms | published ms | 满 RF s | 备注 |
|---|---|---|---|---|---|
| 诊断 | 9→10→9 | 2720 / 422 | 2052 / 344 | 4.40 / 6.54 | `pre_publish_delay=250`：`publish_ms` 270/268（主组 16/16），停写窗口 1082/429 ms |
| 策略 | 9→10→9 | 6809 / 189 | 2152 / 98 | 0.02 / 6.54 | `FirstSuccessor`：grow `activation_ms=4645`（主组 667），停写窗口 4635 ms |
| S3 | 10→10 替换 | 5548 | 4285 | 4.25 | 复制路径：snapshot 2229、replay 4338、verify 2006；四个分层都有样本；停写窗口 2188 ms |
| S4 | 6→9→6 | 6243 / 6929 | 4608 / 5616 | 3.60 / 2.01 | grow 同时新增 3 个成员；shrink `direct_merge=false`（一次移除 3 个成员时 coverage 不成立），停写窗口 2335/2827 ms |

`complete ms` 列中 `/` 两侧分别是 grow 与 shrink（S3 只有一次替换）。策略组 `full_rf_seconds=0.02` 是因为 `activation_ms` 已经等了跟随副本；两份时间不能相加。

### 1M 主组（1 次）

| 阶段 | complete ms | published ms | 满 RF s | snapshot | replay | pause | verify | publish | activation | RangeBusy | 停写窗口 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| grow | 2800 | 2136 | 10.60 | 1112 | 2202 | 11 | 984 | 16 | 646 | 37 | 1184 |
| shrink | 191 | 89 | 12.37 | 0 | 0 | 8 | 49 | 15 | 87 | 16 | 146 |

预载 1M 键约 30 s。`Complete` 之后到满 RF 约 11–12 s，是这一轮 100k 主组（4.45/6.58 s）的约 2 倍；`post` 阶段 GET p99 从变更前的 1.12 ms 升到 9.4 ms，说明 RF 修复与负载争用。

### 迁移量（阶段 6-2）

每个 transition 记录 `migration`：owner 在 base 与 target 拓扑之间发生变化的键数、比例、协调器实际搬动的 range 数与快照记录数。迁移率 = `keys_moved / keys`，direct merge 不产生 range，但键仍然换主。

| 变更 | 键数 | owner 变化 | 迁移率 | range 数 | snapshot records |
|---|---|---|---|---|---|
| 9→10 | 100,000 | 10,508 | 10.51% | 128 | 10,670 |
| 9→10 | 1,000,000 | 105,296 | 10.53% | 128 | 105,460 |
| 10→11 | 100,000 | 6,446 | 6.45% | 128 | 6,611 |
| 10→11 | 1,000,000 | 64,761 | 6.48% | 128 | 64,926 |
| 10→9（direct merge） | 100,000 | 10,508 | 10.51% | 0 | 0 |

9→10 的 10.51% 接近 1/(9+1)：新增一个节点拿走环上约 1/10 的键；三次重复完全相同，因为同一 seed 的环是确定的。10→11 的 6.45% 是新增节点在这个 seed 的环上实际占的份额（期望 1/11 = 9.09%；11 节点、V=128 的 `latency` 运行测得 node-11 拥有 6,446 个键，与迁移量一致）。阶段 3/6 的「N≥10 时迁移率 ≤10%」由 10→11 的 6.45% 满足。课程验收指标的完整对照见 [acceptance-metrics.md](acceptance-metrics.md)。

### 开环尾延迟组（1M 键，offered 50k ops/s，3 次）

> **复现条件**：这一组必须在空闲主机上跑。脚本现在按 §9 传 `--keys 1000000`；早期脚本把它误写成 100k（见 §9 注）。在桌面进程占用 CPU 的主机上，第二个 transition 开始前要校验上一个 transition 的 cohort（1M 键约 255k 个键，满载下每次 GET 约 40 ms），需要约 43 s，超过 33 s 的一次性写池：shrink 的写在 change 开始前就耗尽，没有 `RangeBusy` 证据，运行被 §8 的有效性检查判为 invalid。这是检查按设计生效，不是代码错误。下表是 `d0b160f` 在空闲主机上的记录，本轮没有重跑。

| 指标 | open-1 | open-2 | open-3 |
|---|---|---|---|
| `issued` / `late` | 3,510,859 / 451 | 3,397,382 / 7,412 | 3,538,894 / 297 |
| grow `complete_ms` | 2364 | 2400 | 2058 |
| grow 停写窗口 ms | 843 | 861 | 761 |
| grow `RangeBusy` | 9,658 | 9,998 | 8,335 |
| grow 满 RF s | 11.1 | 11.3 | 11.7 |
| shrink `complete_ms` | 151 | 140 | 141 |
| shrink 停写窗口 ms | 142 | 117 | 129 |
| shrink `RangeBusy` | 1,536 | 2,933 | 1,557 |
| shrink 满 RF s | 13.8 | 10.7 | 14.7 |

`late` 是驱动发不出去而丢弃的请求（占 offered 的 0.01%–0.22%），不是集群错误；三次运行的应用层错误均为 0。所有 `late` 都落在发布之后的秒窗口里（每次只有 1 个或几个秒），原因见下。

GET 延迟（按请求发起时所在阶段；操作数为 3 次合计，百分位取 3 次中位数）：

| 阶段 | 操作数 | p50 | p99 | p999 |
|---|---|---|---|---|
| `pre` | 1,229,896 | 73 µs | 275 µs | 534 µs |
| `copying_snapshot` | 114,492 | 111 µs | 1.5 ms | 3.6 ms |
| `verifying` | 93,117 | 237 µs | 4.7 ms | 6.9 ms |
| `published → complete` | 138,046 | 140 µs | **2.49 s** | 2.98 s |
| `post`（含 RF 修复） | 7,319,850 | 130 µs | 19.5 ms | 38.5 ms |

`published → complete` 的 p99 和 `post` 的 p99 是同一件事：**发布之后立刻开始的 RF 修复**。修复期间整个集群开始排队，读请求把 in-flight 推到接近 4096（单秒采样 1.8k–3.9k），超出的请求按 §5 计为 `late`。逐秒采样（open-2 最明显）显示 shrink 发布后约 7 s 内 GET p50 升到 37–55 ms、p99 约 105–147 ms，期间完成的 `put_rewrite` 只有个位数、耗时 2.3–9.6 s；open-1 与 open-3 的退化只持续约 1 秒，分别丢弃 451 和 297 次。作为对照，闭环 1M 主组以 200 写/秒跑同一窗口，`post` 阶段 GET p99 只有 4.6 ms、`max` 56 ms：修复退化随写入负载放大，不是单纯的迁移错误。开环组 `pausing_writes` 里 `put_rewrite` 486 次、p50 513 ms、p99 3.7 s，是栅栏本身（而不是修复）造成的阻塞。

### 有效性（§8）

| 项 | 结果 |
|---|---|
| 主组 `pre_publish_delay_ms = 0` | `scaling` 模式默认即 0，逐次 manifest 记录为 0 ✔ |
| 最后一次 `Complete` 前无池耗尽 | 本轮 11 次闭环运行全部成功（耗尽会判 invalid）✔；开环组 3 次因主机负载判 invalid，见上一节 |
| 适用分层都有实际样本 | 每次 transition 的 `touched_strata` 覆盖 `expected_strata`，运行内强制检查 ✔ |
| 停写窗口内至少一次 `RangeBusy` | 本轮 100k 主组 grow 26–30、shrink 14–16；1M 主组 grow 37、shrink 16；诊断/策略/S3/S4 同样>0 ✔ |
| 阶段 × 分层覆盖 | 只报告，不作为判定条件 |

### 硬性标准

| 项 | 结果 |
|---|---|
| change 正常完成并发布 | 本轮 11 次闭环运行全部 `Complete`，无 `Aborted` |
| 正确性违规为 0 | 全量校验、S1 后中间校验、读己之写/删、发布后复读均 0 违规；探针违规计数 0 |
| 进程无意外退出 | 被移除节点在宽限期内正常退出（`removed_node_exited`），无 `unexpected exit` |

### 局限

- 共享桌面主机，桌面进程同时运行；本轮桌面负载更高（`loadavg` 约 5），绝对吞吐低于更早的 `d0b160f` 记录（主组 grow `complete_ms` 2772 vs 1855），重复运行的波动同样受它影响。
- S3/S4/诊断/策略组各只有 1 次重复，只能作为量级参考，不能当作分布。
- 开环组在 RF 修复窗口出现负载丢弃，说明“稳态容量 60%”在修复期并不成立；容量基线只在无迁移、无修复的稳态下测得。
- 计划 §5 中列为可选的“原始 owner RPC 探针”没有做：阻塞时长以客户端观测的 `RangeBusy` 窗口和按阶段的写延迟报告，没有把传输与重试等待分开。原始 `results/` 只有本机，不作为共享基线。
- 开环组的一次性写入池必须覆盖整个 transition，包括下一个 transition 开始前对上一个 transition 的延迟校验。这个校验在满载下很慢（1M 键约 43 s），所以开环组对主机负载非常敏感：本轮在忙的主机上 3 次都被判 invalid。100k 键下池窗口只允许数千 ops/s（低于容量的 10%）。它的 post 窗口是 5 s（主组 10 s），以便池覆盖整个 transition。
- 统计功效：每次运行零失败不能推出失败率上界；可用性只报告观察值（本轮闭环运行 0 失败）。

---

## 1. 实验要回答的问题

在持续混合读写流量下：

1. **正确性**：扩容、缩容期间成功的写入、改写、删除，在迁移后是否仍然成立？读是否出现丢键、旧值、已删键复活？
2. **可用性**：迁移期间 GET、PUT、DELETE 的失败率、超期率和 `RangeBusy` 拒绝分别出现在哪个阶段？
3. **延迟**：迁移各工作步骤（快照复制、changelog 回放、切写、校验、发布、RF 修复）对读/写尾延迟的影响。
4. **重试来源**：客户端重试由哪些应用层错误码、gRPC 状态、连接失败或拓扑刷新触发，集中在哪个阶段。
5. **迁移代价**：各阶段与工作步骤耗时、迁移键/区间数量、发布到完整 RF 的时间、被移除节点退出时间。

与现有 `correctness` 模式的区别：后者只对“会迁移的键”做一次性改写/删除，**没有持续 GET 负载，也没有负载中的延迟、错误和按阶段统计**。

---

## 2. 已核对的代码事实（决定方案形态）

| 事实 | 位置 | 对方案的影响 |
|---|---|---|
| `direct_merge` 要求目标成员数**严格减少**，且目标成员全部来自旧成员 | `crates/hashring-coordinator/src/merge.rs:14` | S2 缩容可走；S3 同时增删**一定不走** |
| 走 `direct_merge` 时清空 `change.ranges` | `crates/hashring-coordinator/src/rpc.rs:443` | 没有快照复制，但仍有停写、追平、发布阶段 |
| `direct_merge` 依次经过 `PausingWrites` → `Verifying` → `ReadyToPublish`；写栅栏在 `PausingWrites` 加上并保持到发布；`Verifying` 等待旧复制流追平 | `crates/hashring-coordinator/src/merge.rs:334-391` | S2 全集群停写的主要耗时在 `Verifying`；该阶段必须标为适用 |
| `direct_merge` 会对**所有旧成员**加写栅栏 | `crates/hashring-coordinator/src/merge.rs:337` | 缩容时未迁移的键也可能被停写，“不动键”不能当对照组 |
| `ReplayingChangelog` 设置后立刻进入 `PausingWrites` | `crates/hashring-coordinator/src/change.rs:123` | 该阶段无法用轮询观测；回放工作实际发生在 `CopyingSnapshot` / `Verifying` 内部 |
| `pre_publish_delay` 在 `ReadyToPublish` 且写已暂停后 sleep，两条路径都生效 | `crates/hashring-coordinator/src/change.rs:179`、`merge.rs:393` | 默认 250 ms 会人为拉长停写；direct merge 时停的是**整个集群**的写，性能组必须设 0 |
| 发布后 `finish_published_change` 重新计算 `migration_timeout` | `crates/hashring-coordinator/src/change.rs:807` | 不能把它当作整个实验的总期限 |
| `correctness` 模式已实现 9→10→9 的编排：初始节点少于峰值、`begin-change` 后启动新节点、等待被移除节点退出 | `crates/hashring-experiment/src/lib.rs:848` | 应扩展现有流程，而不是从零写一套 |
| 客户端已区分 `KnownNotApplied` 与 `MayHaveApplied` | `crates/hashring-client/src/lib.rs:127` | 最终校验必须按操作类型和结果分别定义允许终态 |
| 可重试错误码还包括 `TemporarilyUnavailable`、`ReplicaNotReady`、`OutcomeUnknown` | `crates/hashring-client/src/lib.rs:916` | 重试原因列表需补全 |
| 现客户端只有总 `retry_count`，且包含拓扑刷新 | `crates/hashring-client/src/lib.rs:324` | 需要按原因/通道拆分统计 |

S2 是否真的走 `direct_merge` 还取决于 `can_direct_merge` 的 `merge_proof`；主配置（RF=3、`OwnerOnly`、副本健康）下预期走，若不满足会退回复制路径。运行时记录实际路径，不能预设。

---

## 3. 场景

主配置：**RF=3、`OwnerOnly`、`minimum_admitted_copies=1`、`minimum_healthy_followers=0`、30 s 期限**。`FirstSuccessor` 作为对照。

| 场景 | 拓扑变化 | 实际路径 | 主要观察 |
|---|---|---|---|
| **S1 扩容** | 9 → 10，新增 node-10 | 复制：快照 + changelog 回放 | 读连续性；切写时 `RangeBusy` 的比例与持续时长；复制与负载争用；发布后 RF 修复 |
| **S2 缩容** | 10 → 9，移除 node-10 | 主配置下预期 `direct_merge`：无快照复制，但仍有 `PausingWrites`/`Verifying`；所有旧成员写栅栏，停写覆盖全集群 | 全集群写停顿长度（主要在 `Verifying` 追平旧复制流）；未迁移键是否也被停写；在途请求结果；被移除节点退出；键是否丢失/复活 |
| S3 替换（已实现） | 10 → 10，移除 node-10、加入 node-11 | **一定走复制路径**，被移除节点的区间也要复制 | 正式运行待排；峰值是 11 个同时存活的数据进程 |
| S4 连续迁移（已实现） | 6 → 9 → 6 | 多区间并发迁移 | 正式运行待排；背靠背 change、epoch 抖动、累计影响 |
| **S0 对照（已实现）** | 不变 | 无 | 相同负载与时长下的自然波动，作为 S1/S2 的基线 |

S1 与 S2 在**同一个集群生命周期内连续执行**：预载一次、负载不中断，先扩容再缩容。这样与 TODO #3 的 9→10→9 一致，也避免两套环境差异。S3/S4 单独运行。

两次迁移之间的状态衔接有三条硬性规定：

1. **S1 完成后先做局部 drain 和校验，再开始 S2**。S1 的 `Complete` 和完整 RF 之后：
   - 停止发放 S1 批次的写/删请求；
   - 等待 S1 cohort 的在途逻辑操作全部返回（只针对 S1 的局部 drain，不是全局停止负载）；
   - 按 §4.2 校验 S1 动过的 cohort。

   这段时间主负载继续使用 S2 的预备批次，负载本身不用停；S1 的在途计数要按批次跟踪，确定归零后才能冻结 S1 台账。不能只做最后一次全量校验，否则无法区分违规来自哪次迁移。现有 `correctness` 流程本来就在两次迁移之间做校验（`lib.rs` 约 921–942 行），但它是停掉整个写负载后校验，本方案要改成局部 drain。
2. **S2 使用独立的 cohort 池**。S1 中结果为 `MayHaveApplied` 的键，终态是一个集合（如 `{v0, v1}`），不能拿来当 S2 的 `v0`，必须从 S2 的所有池中排除。
3. **S2 的分层标签以 S1 完成后的拓扑为 base 重新计算**，不能沿用 S1 的 `owner_changed` / `new_target` 等标签。S2 的 base 是 10 节点拓扑，target 是 9 节点拓扑。

---

## 4. 键空间、期望状态与分层

核心原则：每个键的期望终态由**操作类型 + 客户端结果**唯一决定；读写之间不产生无法判定的竞争。

### 4.1 cohort 与负载操作

| cohort | 迁移前 | 负载期间 | 目的 |
|---|---|---|---|
| `read` | 已存在，值 `v0` | 只 GET | 读连续性、丢键、错值 |
| `rewrite` | 已存在，值 `v0` | 每键 PUT 一次到 `v1` | 检测丢失覆盖写 |
| `write_new` | 不存在 | 每键 PUT 一次到 `v1` | 检测丢失新写 |
| `delete` | 已存在，值 `v0` | 每键 DELETE 一次 | 检测删除丢失/复活 |

`read` 与 `delete` 池不相交；`read` 池在整个负载期间不变，任意时刻读到非 `v0` 或 `NotFound` 都直接判失败。

### 4.2 最终校验的允许终态

| 操作 | 成功 | `MayHaveApplied`（结果未知） | `KnownNotApplied`（明确未应用） |
|---|---|---|---|
| `rewrite` | `v1` | `{v0, v1}` | `v0` |
| `write_new` | `v1` | `{不存在, v1}` | 不存在 |
| `delete` | 不存在 | `{v0, 不存在}` | `v0` |
| `read` | `v0` | — | — |

全量校验也按这张表执行。停止发新请求后先执行 **drain**：等待所有在途逻辑操作返回，冻结成功写台账与各操作结果，再做最终校验，避免把仍在途的请求误判为失败。

### 4.3 分层标签

不要用“完全不动的键”当单一对照组。每个 cohort 按以下标签分层，并分别统计**实际打到的操作数**：

- `owner_changed`：owner 变了（会迁移）
- `replicas_changed`：owner 未变，但副本集合变了
- `removed_source`：base owner 是被移除节点
- `new_target`：目标 owner 是新增节点
- `unchanged`：owner 和副本集合都没变（真正的对照）

`read` 使用 `zipf:0.99`，热点未必落在迁移区间，因此必须记录每层实际完成的操作数；每层样本不足时不能外推结论。可复用现有 `moving_key_cohorts` / `moving_keys`（`crates/hashring-experiment/src/lib.rs` 约 1983 行）。

每个场景**适用的分层**不同，有效性只针对适用层，空层是“不适用”而不是“无样本”：

| 场景 | owner_changed | replicas_changed | removed_source | new_target | unchanged |
|---|---|---|---|---|---|
| S1 扩容 | 适用 | 适用 | 不适用（无移除节点） | 适用 | 适用 |
| S2 缩容 | 适用 | 适用 | 适用 | 不适用（无新增节点） | 适用 |
| S0 对照 | 不适用 | 不适用 | 不适用 | 不适用 | 适用 |
| S3 替换（延后） | 适用 | 适用 | 适用 | 适用 | 适用 |

### 4.4 读己之写 / 读己之删（必做）

对每个成功的 `write_new`、`rewrite`、`delete`，由**同一个写入者按顺序**对同一个键做一次 GET，校验：

- 写后读必须返回新值；
- 删后读必须 `NotFound`；
- 重点覆盖正在迁移的键，用于检测“切换后从旧 owner 读到旧值”和“已删键短暂复活”。

单写入者串行操作，不会引入同键并发竞争。该检查从“可选”改为必做，否则结论只能收窄为“不变键读连续性 + 写删最终状态”。

**发布后复读（同样必做）**：只在写完立刻读一次抓不到最主要的风险。“切换后从旧 owner 读到旧值”的窗口在发布前后：一个键若在发布前写入，写完马上读到的仍是旧 owner，无法暴露切换后的问题。因此对 `owner_changed` 层中在每次 `Published` 之前写过/删过的键，在 `Published` 之后（以及 `Complete` 之后）各复读一次。只复读 `begin-change` 之后改动过的键，不要复读整个历史。客户端按缓存拓扑路由，遇到 `Moved` 会刷新并重试，最终由当前 owner 回答，所以这次复读的结果可判定；记录写入 epoch、复读 epoch 与结果，按阶段分别统计。

**探针 GET 必须与主负载分开统计**：读己之写/删和发布后复读打的是被改写或删除的键，这些键均匀分布；主负载的 `read` 是 Zipf 分布。两者混进同一个 GET 直方图会让读延迟失真，也会让实际混合比例偏离 80:15:5。因此：

- 给探针单独打标签（如 `probe_read_after_write`、`probe_read_after_publish`）、单独建直方图，不进入主 GET 统计；实际比例单独报告。
- 发布后复读要限速，或使用单独的 client 和独立并发额度，避免在 `Published → Complete` 这个本来就要测延迟的窗口打出一波突发。建议上限为同时段主 GET 速率的一个小比例（如 5%），并记录实际复读速率和数量。

---

## 5. 负载与 cohort 预算

一次性池子与“持续混合负载”会冲突：预热 + 基线期间可能就把有限的改写/新写/删除样本用完。必须明确四件事：

1. **操作比例**：建议 GET:PUT:DELETE 约 80:15:5；其中 PUT 再分为 `rewrite` 与 `write_new`。实际比例单独统计，不拿配置值冒充。
2. **池的大小**：按“目标速率 × 预计总时长 × 安全系数”生成；`write_new` 预留独立键段，绝不与已用键冲突。
3. **发放方式**：按时间窗口分批发放；**给迁移窗口预留专用批次**，在 `begin-change` 之后才释放。删除池按同样方式分批。
4. **用完后行为**：在最后一次 `Complete` 之前，任何一个池用完都判整次运行 **invalid**，不能“停掉该 cohort 继续跑”。否则迁移后半段的混合比例与 S0 对照及其他重复运行不同，延迟不可直接比较。池按预计最长时长加安全系数生成；不得复用键写入不同值。

负载形态：

| 形态 | 参数 | 用途 |
|---|---|---|
| **闭环（主）** | 并发 64，完整 30 s 期限与重试 | 回答“是否持续服务”。注意：一遇停顿闭环就会少发请求，**并不能免除协调遗漏**，只能减轻。 |
| **开环（复核）** | `--scaling-load-mode open --scaling-open-rate <rate>`；固定速率；目标速率 = 特定策略/拓扑/混合比例下测得的稳态容量的 60%，`--max-in-flight 4096` | 回答“给定 offered load 下的真实尾延迟”。必须写明容量基线来自哪组配置。 |

开环按总量 80:15:5 发放 GET:PUT:DELETE，PUT 再分 60/25/15；池按 `--scaling-pool-seconds` 与该写入速率生成。`late`（驱动发不出去）与 `errors`（集群返回失败）分开报告，不能混为一谈；延迟从请求到期时刻开始计，不从事后实际发出的时刻开始。

**复用边界**：`latency.rs` 只复用直方图和固定速率调度器；不能复用请求执行器，因为它随机选键、PUT 固定写 round 0、删除键可能被重复选中、GET 返回值被丢弃，不满足台账要求。迁移观察必须挂在**同一段不中断的负载**上，不能按阶段重置统计或排空请求。

可选**原始 owner RPC 探针**：绕过客户端重试，直接向迁移中区间的 owner 发 PUT/GET，测出真实切写阻塞时长；否则重试会把停顿“抹平”。

---

## 6. 单场景时间线

```
启动 9 个节点（S1）→ 预载 100k 键 → 等待目标 epoch 的完整 RF
→ 启动持续负载 → 预热 10 s → 记录基线窗口 10 s
→ begin-change（10 成员）→ 启动 node-10 → 等待监听
→ execute-change，负载不停
→ 等待 Published → activation_ready → Complete → 完整 RF
→ 记录迁移后窗口
→ 停止发放 S1 批次 → 等待 S1 cohort 在途请求返回（局部 drain）
→ 按 §4.2 校验 S1 cohort（S2 预备批次继续跑）
→ 以 10 节点拓扑重新计算 S2 分层，准备 S2 独立池
→ begin-change（回到 9 成员）→ execute-change，负载不停
→ 等待 Published / Complete / 被移除节点退出 / 完整 RF
→ 记录迁移后窗口 → 停止发新请求 → drain → 冻结台账 → 全量校验
```

每个 change 都单独记录五个时间点；S1 与 S2 不合并计时。

**时间点要分开记录五个**：开始执行、发布（Published）、`activation_ready`、`Complete`、首次与稳定达到完整 RF。RF 修复不是 `MigrationPhase`，不能塞进阶段统计。`wait_for_full_rf` 需要增加**目标 epoch 检查**，避免读到旧拓扑的“已满 RF”。实现上每个 transition 写 `published_ms`、`complete_ms`、`full_rf_seconds`，与 `work_steps_ms` 的 `publish_ms`/`activation_ms` 一起给出这五个点。

**阶段边界精度**：轮询间隔决定边界误差（建议 2–5 ms），报告中要写明。`migration_timeout` 会在发布后重新计时，实验本身另设总期限。

**延迟归因**：按请求**发起时**所在的阶段统计；开始和结束跨越阶段的请求单独展示。按完成时间归因会把停写期间的停顿算到发布之后。

**`pre_publish_delay_ms`**：主性能组显式设为 **0**。该延迟在复制路径与 direct merge 路径上都生效（`change.rs:179`、`merge.rs:393`）；S2 走 direct merge 时，它停的是**整个集群**的写，不只是迁移区间，所以 delay=0 对 S2 更重要。带 250 ms 延迟的运行只作为诊断组，单列，不与主组比较写延迟。delay=0 是主组的**运行前提**（见 §9），不列为通过标准。

---

## 7. 指标与阶段适用性

### 7.1 阶段适用性

不再使用“每阶段至少 1000 次成功操作”的统一门槛，改为逐阶段标记 **适用 / 不适用 / 未观测**：

| 阶段 | S1 扩容 | S2 缩容（direct merge） | 应看到的证据 |
|---|---|---|---|
| `CopyingSnapshot` | 适用 | 不适用 | 读成功；写成功或可重试拒绝 |
| `ReplayingChangelog` | 瞬时，基本观测不到 | 不适用 | 不单独判定；回放耗时由 coordinator 工作步骤计时的 `changelog_replay_ms` 给出 |
| `PausingWrites` | 适用 | 适用 | **写请求尝试并被 `RangeBusy` 拒绝**，不要求成功写 |
| `Verifying` | 适用 | **适用** | 写被 `RangeBusy` 拒绝、读成功；S2 的停写主要耗时在这里（等旧复制流追平） |
| `ReadyToPublish` | 适用（delay=0） | 适用（delay=0） | 读成功；S2 的写仍被栅栏拒绝 |
| `Published → Complete` | 适用 | 适用 | 读写恢复；移除节点退出 |

停写窗口的正确证据是“有写尝试且被拒绝”，不是“有大量成功写”；S2 不走复制路径，不能要求 `CopyingSnapshot` 有样本，但 `Verifying` 必须适用。

若要拆分复制、回放、校验各自的成本，**光看 phase 不够**。协调器现在在每次 change 上记录工作步骤耗时，随 `TopologyChangeSnapshot.timings` 返回，实验写进 `<label>_transition.work_steps_ms`：

| 字段 | 含义 |
|---|---|
| `snapshot_copy_ms` | 所有 range 的 snapshot 复制（含 dedup）耗时之和，不含该 range 的 changelog 回放；并发迁移时可能大于 `CopyingSnapshot` 阶段墙钟 |
| `changelog_replay_ms` | 所有 range 的 `replay_changelog` 耗时之和；不含之后的 digest 往返与 destination commit（它们属于 `verify_ms`）；并发迁移时可能大于所在阶段 |
| `pause_writes_ms` | `PausingWrites` 阶段墙钟（direct merge 时含加栅栏；policy change 时含向每个成员发暂停栅栏） |
| `verify_ms` | `Verifying` 阶段墙钟（复制路径含最终回放、digest 与提交；policy change 含等待 follower 追平的屏障） |
| `publish_ms` | 提交新拓扑的工作耗时（目标实例校验、状态锁与一致性检查），不含 `pre_publish_delay` 观测窗，也不含承载该计时的这次持久化写本身 |
| `activation_ms` | 提交后 activation/RF repair 屏障与目标拓扑安装；不含清理 source range、停被移除节点和 `post_publish_delay` |
| `merge_proof_ms` | direct merge 的 coverage/proof 计算 |
| `catch_up_ms` | direct merge 等旧复制流追平的循环 |

这些是 coordinator 侧的墙钟，不含客户端重试等待，也不含 `wait_for_full_rf` 在 `Complete` 之后的收敛时间。字段为 0 表示该路径不经过这一步（例如 S2 direct merge 的 `snapshot_copy_ms`/`changelog_replay_ms`）。计时与阶段变更在同一次持久化写中落库：发布时 `publish_ms` 随 `Published` 阶段一起写入，activation 之后 `activation_ms` 随 `CleaningUp`/`Complete` 一起写入；发布之前的已完成步骤随下一阶段一起落库，direct merge 的 proof 计时在证明完成后写入，因此状态查询和被 abort 的 change 都保留已测步骤。中断重试在清理期间保留计时，真正开始新一轮复制时清空上一轮计时。因此 `change-status`、重复的 `execute-change`、以及重启后从 redb 读到的记录，都和 `execute-change` 的返回值一致；不存在"阶段已落库、计时还没写"的窗口。另外 `<label>_transition` 还有 `published_ms`、`published_ms_source`、`complete_ms`、`full_rf_seconds`：前两个从 change 开始计时，`full_rf_seconds` 从 `Complete`（含之后等被移除节点退出的时间）到目标 epoch 满 RF。短 change 可能从 `Verifying` 直接跳到 `Complete`，此时探针仍会启动，`published_ms` 取完成时刻，`published_ms_source` 记为 `completion_fallback`，正常观测到发布时为 `observed`。

恢复路径的 `activation_ms` 只覆盖"恢复时的重做"：在 `Published`/`CleaningUp` 恢复的 change 不会重测发布，`publish_ms` 保持首次发布写入的值；`activation_ms` 度量的是重新安装与重新激活，而不是原始 activation。租约等待也不计入（它是一段安全等待）。


### 7.2 负载指标

- 尝试数、成功数、失败数（应用层 `ErrorCode` / gRPC status / 连接失败 / 拓扑刷新分开）
- 重试原因：`Moved`、`RangeBusy`、`Unavailable`、`ResourceExhausted`、`TemporarilyUnavailable`、`ReplicaNotReady`、`OutcomeUnknown`、`LeaseExpired`、`Rpc`（传输层状态）、`Connect`（建连失败）、`TopologyRefresh`
- 主负载 GET、PUT、DELETE 的 p50/p95/p99/max 与 `max_observed`；探针 GET（读己之写/删、发布后复读）单独直方图，不并入主负载
- 每秒窗口：`epoch`、`issued`、`late`、`achieved_rate`、in-flight
- 切写窗口的写尝试数、拒绝数、停写时长；首个成功写时间
- **停写窗口的定义以客户端实际观测为准**：按区间（S2 是全集群）记录第一次和最后一次 `RangeBusy` 的时间，再取此后第一个成功写，用这三者定义窗口；phase 只做粗粒度标注。栅栏在节点收到 RPC 后才生效，与 coordinator 的 phase 切换存在时差，再叠加轮询误差，不能拿阶段边界当停写时长。实现上由 scaling 模式每 5 ms 采样客户端的 `RangeBusy` 计数与成功写计数；每次成功写还记录完成时观察到的拒绝计数，避免把同一采样间隔内先于拒绝的成功写误当成恢复，写入 `<label>_transition.stop_window`（`first_range_busy_ms`、`last_range_busy_ms`、`first_write_ok_ms`、`observed_ms`、`poll_ms`）。每个 transition 的窗口以它开始时的累计计数为基线，所以第二个 transition 不会把第一个的拒绝算成自己的，`first_range_busy_ms` 也不会被钉成 0。
- 发布后复读在观察到 `Published` 的那一刻启动（限速、独立 client），不是整个 transition 结束后的一次终态复查，否则发布瞬间的短暂陈旧读会自愈、探针看不到。
- 发布后复读必须真的读到键。探针等待的是"持续有进展"而不是固定时长：每次读都受客户端 operation timeout 限制，所以一个还在读的探针一定会在该时限内完成一个键；连续 `operation timeout + 间隔 + 10 s` 没有新键才算卡住。空闲时钟从探针启动时起算（不是从运行等待探针时起算），读到一个键就把截止时间往后推同样的时长，所以早期就卡住的探针会在运行结束前被放弃，并记一条 `scaling_after_publish_probe_stuck` 事件。只有值匹配或确认预期的缺失才推进读取数；GET 失败、意外缺失或错误值都会使探针和运行失败，即使后续终态复核恢复也不算通过。每个 change 的探针读取数写入 `scaling_after_publish_probes.keys_read`；读取数为 0（发布窗口内没有落到迁移分层的成功写）即判该次运行 invalid，而不是当作"没有违规"。
- 重试总数与按原因计数必须对得上：`retry_count()` 等于 `retry_reasons()` 之和。建连失败记 `Connect`，可重试的 gRPC 状态记 `Rpc`，两者不再合并。
- 开环组的实际写入速率来自 `--scaling-open-rate` 的分层拆分，`scaling_pool_plan.cohort_write_rates` 记录 rewrite/new/delete 各自的实际速率；`write_rate` 在开环下为 `null`，因为 `--scaling-write-rate` 在开环下不生效。分层拆分是整除（闭环 60/25/15，开环 12/5/3），所以 `cohort_write_rates.total` 可能比配置值略小（例如 `--scaling-write-rate 50` 实际是 30+12+7=49）；`write_rate` 记的是配置值，`cohort_write_rates` 记的是取整后的实际值。最小的一层必须至少分到 1 次操作/秒，否则该 cohort 没有键；这一点在参数校验阶段就会拒绝（开环 `open_rate * 3 / 100`，闭环 `write_rate * 15 / 100`）。
- 读连续失败最长间隔
- 每层 cohort 的实际操作量与结果分布
- 每次 change 的迁移量：`<label>_transition.migration` 的 `keys`、`keys_moved`、`rate`、`ranges`、`snapshot_records`
- 每节点 owner 操作计数、CPU、owned keys/ranges，观察负载转移

### 7.3 正确性指标

- 按 cohort × 分层标签：缺失、错值、复活删除的计数
- 成功写/删台账（键、值、操作结果、返回版本、epoch）
- `MayHaveApplied` / `KnownNotApplied` 分别归类，不混入成功台账

---

## 8. 通过标准

**运行有效性（不满足判 `invalid`，不判 pass/fail）**

1. 主组 `pre_publish_delay_ms = 0`（运行前提，见 §9）。
2. 在最后一次 `Complete` 之前没有任何 cohort 池耗尽。
3. 整个迁移窗口内，**该场景适用的**每个分层都有实际操作样本（适用层见 §4.3；不适用层不计入判定）。
4. 停写窗口内至少有**一次写请求被 `RangeBusy` 拒绝**。
5. 阶段 × 分层的覆盖情况只报告，不作为判定条件。S1 的 `PausingWrites` 可能只有几毫秒，`read` 又是 Zipf 分布，要求每个适用阶段 × 每个分层都有样本大概率会误判 `inconclusive`。

**硬性（可判 pass/fail）**

1. change 正常完成并发布，未进入 `Aborted`。
2. 正确性违规为 0：按 §4.2 的允许终态逐项成立，包括 S1 后的中间校验与每次 `Published` 后的复读。
3. 所有进程无意外退出；被移除节点在宽限期内正常退出。

**软性（报告，不单独作为“持续可用”的证明）**

- 读/写成功率、`deadline` 数、p50/p95/p99、重试原因分布
- 停写窗口长度、`RangeBusy` 比例、首个成功读写时间
- 五个时间点与完整 RF 恢复时间
- S1 后 owned keys 的最大/均值、每节点负载转移
- 与 S0 对照运行的差值

注意统计功效：1000 次零失败**不能**推出失败率 < 0.1%。可用性结论只能写成“在该负载与样本量下观察到 X 次失败”，不能写成“持续可用”。若要支撑严格失败率，需要显著增加样本量或按二项分布给出置信区间。

---

## 9. 运行矩阵

| 组 | 场景 | 策略 | 负载 | 键数 | 重复 | 备注 |
|---|---|---|---|---|---|---|
| 主组 | S1→S2 往返 | OwnerOnly | 闭环 | 100k | 3 | `pre_publish_delay=0` |
| 对照 | S0 不迁移 | OwnerOnly | 闭环，同时长 | 100k | 3 | 区分自然波动 |
| 诊断 | S1→S2 往返 | OwnerOnly | 闭环 | 100k | 1 | `pre_publish_delay=250`，只诊断 |
| 策略对照 | S1→S2 往返 | FirstSuccessor | 闭环 | 100k | 1 | 量化等待 follower 的代价 |
| 尾延迟 | S1→S2 往返 | OwnerOnly | 开环 60% 容量 | 1M | 3 | 容量与速率见 §0.1 |
| 替换 | S3 替换 | OwnerOnly | 闭环 | 100k | 1 | 峰值 11 个数据进程 |
| 连续迁移 | S4 6→9→6 | OwnerOnly | 闭环 | 100k | 1 | 多区间并发迁移 |
| 大规模 | S1→S2 往返 | OwnerOnly | 闭环 | 1M | 1 | §0.1 的 1M 主组 |

- 每次运行全新集群、独立输出目录；正式运行 `--require-clean-source`；主组 seed=1，方差大时补 seed=2。矩阵入口是 `scripts/scaling-matrix.sh`。
- 100k 键、闭环 200 写/秒时每个 transition 只有 25k 可写键，`--scaling-pool-seconds` 取 120 留出约 1.4 倍余量；开环高得多的写速率要用更短的池窗口，否则池在 change 完成前就会耗尽并判 invalid。
- worker 线程预算按**同时存活的数据进程峰值**计算：S1/S2 是 10，S3 是 11。端口也一样。
- 机器上不跑其他负载；对照组的节点数、线程预算与主组一致（本次主机是共享桌面机，见 §0.1 局限）。
- cohort 池按“预计最长时长 × 安全系数”生成；最后一次 `Complete` 前耗尽即判该次运行 invalid，不进入统计。

本次正式矩阵（`b0ce975`）的实际参数：主组、对照、诊断、策略与 S3/S4 均为 100k 键、闭环 200 写/秒、`scaling-pool-seconds=120`；S3 用 `--nodes 11`（10→10），S4 用 `--nodes 9 --initial-nodes 6`。开环组按上表为 1M 键、`--scaling-open-rate 50000`、`--scaling-pool-seconds 33`、`--scaling-post-seconds 5`、`--max-dedup-bytes 268435456`，重复 3 次；容量基线用同一配置的 `latency` 模式测得（§0.1）。矩阵脚本的 `RUN_GROUPS` 默认覆盖全部七组，开环组用 `OPEN_KEYS`（默认 1000000）而不是 `KEYS`。

---

## 10. 实现改动（在现有 correctness 流程上扩展）

1. **扩展现有编排**：`run_cluster` 已支持初始节点数 ≠ 峰值、`begin-change` 后启动新节点、等待被移除节点退出（`lib.rs:848` 起）。增加可配置的 `initial_nodes` 和场景选择，不要把这段重写。
2. 新增 `scaling` 模块：
   - cohort 分层（owner_changed / replicas_changed / removed_source / new_target / unchanged），并标注每个场景的适用层；
   - 四类负载任务 + 读己之写/删 + 发布后复读；
   - 探针 GET 单独标签/直方图/限速，不混入主负载统计；
   - 按时间窗口发放、迁移窗口预留、池耗尽即 invalid；
   - S1/S2 独立池，S2 以 S1 后拓扑重新分层；
   - S1 批次局部 drain（等在途归零）与最终全局 drain，按 §4.2 校验；
   - 阶段观察与每秒采样。
3. 复用 `latency.rs` 的直方图与开环调度器；新写满足台账要求的执行器。
4. 客户端增加**按原因/通道的重试计数**与应用层错误分类，拓扑刷新单独计数；保留 `MutationOutcome` 区分。
5. coordinator **工作步骤计时**已落地：每次 change 在 coordinator 内记录快照复制、changelog 回放、暂停、finalize/digest、发布与 activation 的墙钟，写入 `TopologyChangeSnapshot.timings`；experiment 将其写入 `<label>_transition.work_steps_ms`。`changelog_replay_ms` 是各 range 之和，并发时会大于所在阶段；`snapshot_copy_ms` 同样按 range 求和，且只算快照复制本身，不含该 range 的回放。计时随阶段变更在同一次持久化写中落库。
6. `wait_for_full_rf` 增加目标 epoch 检查；汇总五个时间点（`published_ms`、`complete_ms`、`full_rf_seconds`）。
7. 更新 `docs/experiments.md`；实验完成后把配置、结果、限制写入 Git 跟踪文档（如 `docs/scaling-under-load.md`），并回链 `docs/TODO.md` 第 3 项；原始 `results/` 不纳入 Git。
8. 测试：§4.2 期望状态表、分层选择与场景适用层已有单元测试；S1→S2 状态衔接（延迟校验、独立池）、停写窗口与开环局部 drain 由 `tests/process_cluster/experiment.rs` 的往返测试与开环往返测试覆盖。

---

## 11. 风险与混杂因素

- **S2 全集群写栅栏**：未迁移的键也可能停写，`unchanged` 层不是安全对照；必须实际测量，而不是假设。
- **S3 一定复制**，且峰值 11 个数据进程；预算和端口按 11 算，不能按 10 算。
- **阶段轮询分辨率**：`ReplayingChangelog` 看不到，边界只能精确到轮询间隔；coordinator 工作步骤计时已提供更细的回放/校验成本，但 `changelog_replay_ms` 是并发 range 之和，不能当作墙钟。S2 的停写时长以客户端观测到的 `RangeBusy` 为准，不以阶段边界为准。
- **单机共享 CPU**：复制、digest 与负载争用；worker 预算固定。
- **客户端重试掩盖停顿**：逻辑延迟包含重试等待；原始 owner RPC 探针用于分离真实阻塞。
- **开环 `late` vs `errors`**：容量不足导致的丢弃不等于集群错误。
- **开环下 `range_busy` 可能为 0**：低速开环下若没有写请求命中停写窗口，§8 的有效性检查会判 invalid；这不代表迁移错误。本次 50k ops/s 的开环组每次都有 8k+ 次拒绝，没有触发该情况。
- **开环一次性写入池**：池的键来自预载键空间，一个 transition 的池必须撑满整个 transition（预热、change、满 RF、post 窗口），因此高 offered rate 需要大键空间；100k 键下最多只有数千 ops/s。
- **统计功效**：小样本零失败不能证明低失败率；可用性只作观察值。
- **协调器单点**：本实验不验证协调器 HA。

---

## 12. 已确认的选择

1. 主规模用 **9↔10**，与现有结果和 TODO #3 可比。
2. **闭环为主、开环复核**；按 §5/§8 修正措辞和统计口径。
3. 直接在现有 correctness 迁移流程上扩展实现 `scaling`，不先做手工原型。

下一步：把 S3、S4、诊断与策略对照补到三次重复；实现 §5 的原始 owner RPC 探针，把客户端重试等待与真实阻塞分开；若要开环组回到 100k 键，需要让一次性写入池不再受预载键空间限制。正式矩阵的配置、结果与限制已写回 §0.1，并回链 `docs/TODO.md` 第 3 项。
