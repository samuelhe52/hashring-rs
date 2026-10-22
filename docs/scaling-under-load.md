# 扩缩容期间读写实验（方案 v4）

> 状态：方案已固定，按本文实现 `scaling` 模式；实验完成后再在文末补结果与限制。
> 本文整合了三轮评审意见，只保留中高价值项。

## 0. 结论先行

- 主线收窄为 **同一个集群里 9→10→9 往返一次**（S1 扩容接 S2 缩容），对齐 `docs/TODO.md` 第 3 项。
- **S3 替换、S4 多节点连续迁移往后排**；先保证主线可解释、可重复。
- 增加一组**相同配置、不做迁移的对照运行**，用来区分正常波动与迁移影响。
- 主性能组必须显式设置 `pre_publish_delay_ms = 0`，否则会人为拉长停写窗口、污染写尾延迟。
- “持续可用”的结论不能由少量零失败样本推出；硬性通过标准只放正确性，可用性和延迟作为可观测结果报告。

## 0.1 实现状态

第一阶段已实现并通过两次试运行。

5 节点、5,000 键的小试点（debug）：

```sh
cargo run -- experiment --mode scaling --scaling-scenario round-trip \
  --nodes 5 --keys 5000 --concurrency 16 \
  --scaling-write-rate 20 --scaling-pool-seconds 30 \
  --warmup-seconds 3 --scaling-post-seconds 3 \
  --output results/scaling-pilot
```

默认规模的一次试运行（release，9→10→9，100k 键，RF=3，`OwnerOnly`）：

```sh
target/release/hashring-rs experiment --mode scaling \
  --output results/scaling-pilot-100k-v2
```

结果：成功；总耗时 50.0 s；扩容 2.34 s（`direct_merge=false`），缩容 0.29 s（`direct_merge=true`）；约 507 万次操作；全量校验与探针 0 违规；扩容/缩容分别观察到 24/16 次 `range_busy` 重试。同一环境的前一次试运行扩容耗时 22.94 s，单次试运行方差很大。这是单次试运行，没有对照组，也不是正式结果。

- 已实现：`round-trip` / `grow` / `shrink`、四类 cohort、按速率发放、局部 drain、S1 后中间校验、最终全量校验、读己之写/删、发布后复读、按阶段与每秒指标、按原因的重试计数。
- 尚未实现：开环复核组、S0 对照的运行入口、S3/S4、100 万键规模、coordinator 工作步骤计时（按 §10 第 5 条延后）、正式三次重复矩阵。
- 试点结论只能说明机制跑通，不作为正式结果。

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
| S3 替换（延后） | 10 → 10，移除 node-10、加入 node-11 | **一定走复制路径**，被移除节点的区间也要复制 | 只在主线稳定后做；峰值是 11 个同时存活的数据进程 |
| S4 连续迁移（延后） | 6 → 9 → 6 | 多区间并发迁移 | 背靠背 change、epoch 抖动、累计影响 |
| **S0 对照** | 不变 | 无 | 相同负载与时长下的自然波动，作为 S1/S2 的基线 |

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
| **开环（复核）** | 固定速率；目标速率 = 特定策略/拓扑/混合比例下测得的稳态容量的 60%，`max_in_flight=4096` | 回答“给定 offered load 下的真实尾延迟”。必须写明容量基线来自哪组配置。 |

开环的 `late`（驱动发不出去）与 `errors`（集群返回失败）分开报告，不能混为一谈。

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

**时间点要分开记录五个**：开始执行、发布（Published）、`activation_ready`、`Complete`、首次与稳定达到完整 RF。RF 修复不是 `MigrationPhase`，不能塞进阶段统计。`wait_for_full_rf` 需要增加**目标 epoch 检查**，避免读到旧拓扑的“已满 RF”。

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
| `ReplayingChangelog` | 瞬时，基本观测不到 | 不适用 | 不单独判定；回放耗时暂无法单独测，试点后再决定是否加 coordinator 工作步骤计时 |
| `PausingWrites` | 适用 | 适用 | **写请求尝试并被 `RangeBusy` 拒绝**，不要求成功写 |
| `Verifying` | 适用 | **适用** | 写被 `RangeBusy` 拒绝、读成功；S2 的停写主要耗时在这里（等旧复制流追平） |
| `ReadyToPublish` | 适用（delay=0） | 适用（delay=0） | 读成功；S2 的写仍被栅栏拒绝 |
| `Published → Complete` | 适用 | 适用 | 读写恢复；移除节点退出 |

停写窗口的正确证据是“有写尝试且被拒绝”，不是“有大量成功写”；S2 不走复制路径，不能要求 `CopyingSnapshot` 有样本，但 `Verifying` 必须适用。

若要拆分复制、回放、校验各自的成本，**光看 phase 不够**：需要 coordinator 在各工作步骤内计时（快照复制、changelog 追赶、watermark 暂停、finalize/digest、activation）。该计时有生产代码成本，**放到试点之后单独做**（见 §10 第 5 条）；在此之前成本结论只到阶段级别。

### 7.2 负载指标

- 尝试数、成功数、失败数（应用层 `ErrorCode` / gRPC status / 连接失败 / 拓扑刷新分开）
- 重试原因：`Moved`、`RangeBusy`、`Unavailable`、`ResourceExhausted`、`TemporarilyUnavailable`、`ReplicaNotReady`、`OutcomeUnknown`、`LeaseExpired`，以及 transport/connect/topology refresh
- 主负载 GET、PUT、DELETE 的 p50/p95/p99/max 与 `max_observed`；探针 GET（读己之写/删、发布后复读）单独直方图，不并入主负载
- 每秒窗口：`epoch`、`issued`、`late`、`achieved_rate`、in-flight
- 切写窗口的写尝试数、拒绝数、停写时长；首个成功写时间
- **停写窗口的定义以客户端实际观测为准**：按区间（S2 是全集群）记录第一次和最后一次 `RangeBusy` 的时间，再取此后第一个成功写，用这三者定义窗口；phase 只做粗粒度标注。栅栏在节点收到 RPC 后才生效，与 coordinator 的 phase 切换存在时差，再叠加轮询误差，不能拿阶段边界当停写时长。
- 读连续失败最长间隔
- 每层 cohort 的实际操作量与结果分布
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
| 尾延迟 | S1→S2 往返 | OwnerOnly | 开环 60% 容量 | 100k | 1–3 | 注明容量基线配置 |
| 延后 | S3 / S4 / 1M | OwnerOnly | 闭环 | — | — | 主线稳定后再做 |

- 每次运行全新集群、独立输出目录；正式运行 `--require-clean-source`；主组 seed=1，方差大时补 seed=2。
- **主组 `pre_publish_delay=0` 是运行前提**；不满足的运行不进入主组统计。250 ms 诊断组单独比较。
- worker 线程预算按**同时存活的数据进程峰值**计算：S1/S2 是 10，S3 是 11。端口也一样。
- 机器上不跑其他负载；对照组的节点数、线程预算与主组一致。
- cohort 池按“预计最长时长 × 安全系数”生成；最后一次 `Complete` 前耗尽即判该次运行 invalid，不进入统计。

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
5. coordinator **工作步骤计时**（快照复制、changelog 追赶、暂停、finalize/digest、activation）**放到试点之后单独做**。它会改生产代码，且没有它也能先把结论回答到阶段级别；届时再决定是否值得引入。
6. `wait_for_full_rf` 增加目标 epoch 检查；汇总五个时间点。
7. 更新 `docs/experiments.md`；实验完成后把配置、结果、限制写入 Git 跟踪文档（如 `docs/scaling-under-load.md`），并回链 `docs/TODO.md` 第 3 项；原始 `results/` 不纳入 Git。
8. 为 §4.2 期望状态表、分层选择、S1→S2 状态衔接和 drain 协议补单元测试。

---

## 11. 风险与混杂因素

- **S2 全集群写栅栏**：未迁移的键也可能停写，`unchanged` 层不是安全对照；必须实际测量，而不是假设。
- **S3 一定复制**，且峰值 11 个数据进程；预算和端口按 11 算，不能按 10 算。
- **阶段轮询分辨率**：`ReplayingChangelog` 看不到，边界只能精确到轮询间隔；coordinator 工作步骤计时上线前，回放/校验成本只能回答到阶段级别。S2 的停写时长以客户端观测到的 `RangeBusy` 为准，不以阶段边界为准。
- **单机共享 CPU**：复制、digest 与负载争用；worker 预算固定。
- **客户端重试掩盖停顿**：逻辑延迟包含重试等待；原始 owner RPC 探针用于分离真实阻塞。
- **开环 `late` vs `errors`**：容量不足导致的丢弃不等于集群错误。
- **统计功效**：小样本零失败不能证明低失败率；可用性只作观察值。
- **协调器单点**：本实验不验证协调器 HA。

---

## 12. 已确认的选择

1. 主规模用 **9↔10**，与现有结果和 TODO #3 可比。
2. **闭环为主、开环复核**；按 §5/§8 修正措辞和统计口径。
3. 直接在现有 correctness 迁移流程上扩展实现 `scaling`，不先做手工原型。

下一步：按本方案实现 `scaling` 模式与最小试点（S1→S2 各一次），校准阈值后再跑正式矩阵。
