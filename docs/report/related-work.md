# 相关技术调研：分布式缓存系统的架构与高可用方案

一致性哈希只回答“键大致应该放在哪里”。一个可在线扩缩容、能容忍节点故障的缓存系统，还要回答以下问题：

- 客户端怎么知道某个键在哪个节点上？请求由谁路由？
- “哪些节点在集群里、每段数据归谁”这份信息由谁维护，怎么让所有人看到同一个版本？
- 扩缩容时数据要从一个节点搬到另一个节点，搬的过程中读写还在继续，怎么保证不丢、不乱？
- 一份数据存几份？写入什么时候算成功？节点宕机后由谁接管，会不会丢数据？

本章以 Redis Cluster、Apache Cassandra 和 ScyllaDB 为参照，比较它们在这些问题上的做法。Redis Cluster 是分布式缓存的直接参照；Cassandra 和 ScyllaDB 是分布式数据库，但它们使用 token 环，与本项目的一致性哈希路线更接近。三个系统穿插对比，不单独介绍。

## 0. 基本概念

后文反复用到以下术语，先统一说明。

| 术语 | 含义 |
| --- | --- |
| 节点（node） | 一个存储数据、处理读写的服务进程。 |
| 拓扑（topology） | 集群当前有哪些节点、每段数据归哪个节点负责的完整描述。 |
| owner / primary / master | 某段数据当前的权威负责节点，负责接受写入。三个名称在不同系统中含义相近。 |
| 副本（replica）、follower | 同一段数据在其他节点上的拷贝，用于故障后接管。 |
| 复制因子（RF） | 一段数据一共保存几份。RF=3 表示 owner 加两个副本。 |
| 确认（ACK） | 系统告诉客户端“写入成功”。关键问题是：ACK 之前，写入到达了几份副本？ |
| epoch | 拓扑的版本号，每次拓扑变化加一。版本号大的拓扑覆盖版本号小的。 |
| gossip | 节点之间两两随机交换状态，信息像传闻一样逐渐扩散到全体节点。没有中心，但各节点在一段时间内看到的状态可能不一致。 |
| Raft / 共识 | 让多个节点对一串操作的顺序达成一致的协议，要求多数节点在线。 |

另外要注意：**coordinator 一词在不同地方含义不同。** Cassandra/ScyllaDB 中的 coordinator 指“收到本次请求、负责把它转发给副本的那个普通节点”，是每个请求临时的角色；本项目中的 Coordinator 是一个独立的控制面服务，管理拓扑，不处理普通读写。

## 1. 分片、路由与拓扑管理

### 1.1 为什么需要逻辑分片

最直接的做法是 `node = hash(key) mod N`。问题在于节点数 N 一变，几乎所有键的 `mod N` 结果都会变。例如从 4 个节点扩到 5 个，约 80% 的键需要换节点，对缓存来说相当于大面积失效。

因此工业系统都会加一层**逻辑分片**：键先映射到一个稳定的逻辑分片，再由拓扑决定分片归哪个节点。节点增减时，只需把一部分分片改派给别的节点，其余键的位置不变。三个系统的区别在于逻辑分片的形式。

### 1.2 hash slots、token 环与 tablets

**一致性哈希环（token ring）。** 把哈希值空间首尾相连成一个环。每个节点在环上占据若干位置，称为 token；一个键哈希到环上某点后，沿顺时针方向遇到的第一个 token 所属的节点就是它的 owner。每个 token 负责它与前一个 token 之间的一段区间（range）。新增节点时，新节点只从环上相邻节点手里接过一部分区间，其他区间不受影响。

**虚拟节点（vnode）。** 如果每个物理节点只占一个 token，各节点分到的区间长短可能差别很大，新节点也只会分担一个邻居的负载。让每个物理节点占据很多个 token（虚拟节点），区间就被切得更细、更均匀；新节点加入时会从许多节点各接过一小段，负载变化更平滑。

三个系统的具体做法：

- **Redis Cluster：固定 hash slot。** 键空间固定分为 16384 个 slot，`HASH_SLOT = CRC16(key) mod 16384`，每个 master 负责一部分 slot。这不是一致性哈希环：slot 的数量和边界永远不变，扩缩容时变化的只是 slot 到节点的映射。好处是映射表很小、很直观；代价是分片粒度固定。([Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/))
- **Cassandra：经典一致性哈希 + vnode。** partition key 经哈希得到 token，每个节点在环上占据一个或多个 token。官方文档指出，vnode 让数据分布和扩缩容时的负载更均匀；但 token 越多，一个节点在环上的邻居也越多，多个节点同时故障时，更可能有某段数据的全部副本都落在故障节点上。([Cassandra: Dynamo](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html))
- **ScyllaDB：从 vnode 转向 tablets。** ScyllaDB 继承了 Cassandra 的 token 环，但新建 keyspace 现在默认使用 **tablets**：表被切成多个 tablet，每个 partition 确定性地映射到一个 tablet，每个 tablet 有自己独立的副本位置，可以单独迁移，数据变多时还可以一分为二。这说明逻辑分片本身也可以是动态管理的对象，不必永远绑定在固定的 token 上。([ScyllaDB: Tablets](https://docs.scylladb.com/manual/stable/architecture/tablets.html))

本项目采用 Cassandra 式的一致性哈希 + vnode，没有改用固定 slot。

### 1.3 客户端直接路由与节点转发

知道了“键 → 分片 → 节点”的映射，还要决定由谁来做这个计算。

**Redis Cluster：客户端路由。** Redis 节点不会代理请求。客户端缓存一份 slot → node 映射，正常情况下自己算出 slot，直接连接对应的 master。如果客户端的映射过时，把请求发错了节点，该节点返回 `MOVED <slot> <正确节点>`，客户端据此更新映射并重发。([Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/))

**Cassandra / ScyllaDB：节点转发。** 客户端可以把请求发给集群中任意节点。收到请求的节点成为本次请求的 coordinator：它对 partition key 做哈希，找到对应的 token range 和副本，再把请求转发给这些副本、收集回复后答复客户端。([Cassandra: Dynamo](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html))

| | 客户端路由（Redis） | 节点转发（Cassandra/ScyllaDB） |
| --- | --- | --- |
| 数据路径 | 客户端 → 目标节点，一跳 | 客户端 → coordinator → 副本，可能多一跳 |
| 客户端复杂度 | 要缓存拓扑、处理重定向和拓扑变化 | 客户端较简单 |
| 拓扑过时的后果 | 请求发错节点，被重定向后重试 | 由服务端处理，客户端基本无感 |

实际上，Cassandra 的驱动程序通常也会感知 token 分布，尽量把请求直接发给持有副本的节点，以省掉转发这一跳。两种方案的本质区别是：拓扑的复杂性放在客户端还是服务端。

### 1.4 集中与去中心化的拓扑管理

路由依赖拓扑，那么“当前有哪些节点、每个分片归谁”这份**权威拓扑**由谁维护？这是与路由方式相互独立的另一个问题。

**去中心化：gossip。** Redis Cluster 没有中央协调者。节点之间通过 cluster bus 上的 gossip 传播成员、故障判断和 slot 归属。由于 gossip 是逐步扩散的，不同节点可能暂时持有互相冲突的配置，Redis 用 `currentEpoch`、`configEpoch` 两种版本号解决冲突：epoch 更大的配置胜出。Cassandra 5.0（当前 GA 版本）同样通过 gossip 管理成员和故障检测。

gossip 的好处是没有单点，任何节点都能独立运作；难点是拓扑变更（节点加入、退出、分片易主）需要严格的先后顺序，而在 gossip 上很难保证“所有节点看到同样的变更顺序”。例如两个节点同时加入，可能各自按不同的拓扑开始搬数据。

**集中排序：Raft / 元数据日志。** 近年的趋势是把拓扑变更交给一个有序的机制：

- **ScyllaDB** 已经用 Raft 管理拓扑：每个拓扑操作先写入一个多数节点一致认可的日志，按日志顺序执行。官方文档指出，这使得多个节点可以并发 bootstrap，而在原来基于 gossip 的拓扑下做不到。([ScyllaDB: Raft](https://docs.scylladb.com/manual/stable/architecture/raft.html))
- **Cassandra** 的 [CEP-21 Transactional Cluster Metadata](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21:+Transactional+Cluster+Metadata) 计划在尚未发布的 6.0 版本中，把拓扑和 token 归属从 gossip 移到一个带 epoch 的有序元数据日志中。

这里的“集中”是逻辑上的：ScyllaDB 的 Raft 仍运行在数据节点之间，并不是一台额外的服务器。关键在于，这个控制面只处理低频的拓扑变更，不处理每次读写。**去中心化的数据面并不要求去中心化的控制面。** 拓扑变更频率低，但要求严格有序，交给一个集中排序的机制更容易做对；需要避免的，是让控制面进入每个读写请求的路径，成为性能瓶颈。

## 2. 在线扩缩容与迁移

一致性哈希能算出扩缩容后哪些区间应该换 owner，但没有回答真正的难点：**数据从旧 owner 搬到新 owner 需要时间，这段时间里读写仍在继续，怎么办？**

### 2.1 问题在哪里

设想区间 R 要从节点 A 迁到节点 B。最朴素的做法是“先把 A 上的数据复制到 B，再把拓扑改成 B 负责 R”。问题出在复制过程中：

1. 复制开始，A 把 R 的数据逐个发给 B。
2. 键 `k` 已经被发送过去之后，客户端又向 A 写入了 `k` 的新值。
3. 复制结束，拓扑切换到 B。此时 B 上的 `k` 是旧值，这次写入丢失了。

反过来，如果先切换拓扑再复制，切换后到复制完成之前，B 上还缺数据，读请求会看到本不该出现的“键不存在”。所以在线迁移必须解决两件事：**复制期间的新写入怎么同步到目标**，以及**什么时刻切换归属才安全**。

### 2.2 工业系统的做法

**Redis Cluster（传统方式）：显式的中间状态。** 源节点把 slot 标记为 `MIGRATING`，目标节点标记为 `IMPORTING`，然后逐批把键从源移到目标（移过去的键在源上删除）。迁移期间：

- 键还在源节点上：源节点照常处理；
- 键已不在源节点上：源节点返回 `ASK`，客户端**只把这一次请求**发到目标节点，不更新自己的 slot 映射。

全部键迁完后，slot 正式归属目标节点，客户端之后再发错会收到 `MOVED`，这时才永久更新映射。`ASK` 与 `MOVED` 的区别就是“临时去别处问一次”与“以后都去别处”。这种方式不要求瞬间完成复制，但客户端必须处理 `ASK`，涉及多个键的命令在键被拆散于两个节点时也会失败。([Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/))

**Redis 8.4 原子 slot 迁移（`CLUSTER MIGRATION`）。** 新方式改为整体复制：

1. 源节点发送 slot 的快照，同时通过另一条连接实时发送迁移期间的新写入（增量）；
2. 快照发完、增量积压降到阈值以下后，源节点**短暂暂停客户端写入**，把剩余增量发完；
3. 目标节点接管 slot 归属并广播新配置，源节点恢复服务，客户端此后收到 `MOVED`；
4. 源节点在后台删除已迁走的数据。

迁移过程中客户端一直访问源节点，不会收到 `ASK`。Redis 官方的测试中，迁移期间延迟只有短暂、小幅的上升。([Atomic slot migration with Redis 8.4](https://redis.io/blog/atomic-slot-migration/))

**Cassandra：streaming + 延迟清理。** 新节点 bootstrap 时先分配 token，再从现有副本流式拉取自己将要负责的区间。旧节点不会自动删除已移出的数据，需要运维之后执行 `nodetool cleanup`，因此切换出问题时旧数据仍在。迁移期间遗漏的写入依靠后文 3.2 节介绍的 hints 和 repair 补齐。([Cassandra: Topology Changes](https://cassandra.apache.org/doc/latest/cassandra/managing/operating/topo_changes.html))

**ScyllaDB tablets：后台逐个迁移。** tablet 负载均衡器在后台把 tablet 迁到其他节点或 shard，官方文档称迁移不中断服务；拓扑更新由 Raft 保证一致。以 tablet 为迁移单位，每次只搬一小块，出错时影响范围也小。([ScyllaDB: Tablets](https://docs.scylladb.com/manual/stable/architecture/tablets.html))

### 2.3 三类迁移机制的取舍

| 机制 | 做法 | 优点 | 代价 |
| --- | --- | --- | --- |
| 停写整体复制 | 暂停写入，复制完再切换 | 最容易证明正确 | 停写时间随数据量增长，不算在线迁移 |
| 双写 | 迁移期间每次新写入同时写源和目标 | 目标持续获得最新数据 | 见下文 |
| 快照 + 增量 | 先开始记录增量，再复制快照，然后回放增量；最后短暂停写，追平尾部后切换 | 耗时的复制在正常服务期间完成，停写只覆盖最后一小段尾部 | 需要维护增量日志，并严格安排各步骤的先后顺序 |

**双写**看似简单，实际难点很多：迁移逻辑进入了正常写路径，每次写入都要多写一个节点；写源成功、写目标失败时，要决定重试、回滚还是报错；双写的开始时刻必须早于快照扫描，结束时刻必须晚于切换，这两个时刻本身就难以精确界定；同一个键的两次并发写入在源和目标上的到达顺序也可能不同。

**快照 + 增量**把问题拆成两部分：大部分数据在后台复制，复制期间的新写入先记在日志里，事后按顺序回放。它有一个关键约束：**必须先开始记录增量，再开始扫描快照。** 如果顺序反过来，某个键可能已被快照扫过，而它之后的修改还没开始记录，这次修改就会丢失，也就是 2.1 节的问题。

最后的短暂停写是为了处理“尾巴”：只要写入还在进来，目标就永远差一点点。停写期间把最后几条增量发完、校验两边数据一致，再发布新拓扑。只要尾巴足够短，停写时间就很短，而且只影响正在迁移的区间。

切换之后，**新 epoch 的拓扑是判断归属的唯一依据**。旧 owner 上可能还留着数据，但不能再用它回答请求；持有旧拓扑的客户端请求到旧 owner 时，应被告知“拓扑已变”，刷新后再重试。否则，旧 owner 可能返回已经过时的值。

## 3. 复制与故障恢复

扩缩容是计划内的成员变化；高可用还要应对节点突然宕机、网络分区和副本落后。数据存多份是前提，但多份之间怎么同步、写入何时算成功、故障后听谁的，工业系统有两条主要路线。

### 3.1 primary/replica：Redis Cluster

每个 slot 有一个明确的 master 接受写入，replica 从 master **异步**复制数据：master 执行完写入就回复客户端，之后再把写入发给 replica。

**故障切换。** 节点之间通过 gossip 互相探测。某个 master 在一段时间内被多数 master 判定为失联后，它的 replica 发起选举，获得多数 master 投票者提升为新 master，并以更大的 `configEpoch` 宣告自己对这些 slot 的所有权，覆盖旧配置。

**丢失已确认写入的窗口。** 异步复制意味着下面的情况可能发生：

1. 客户端向 master 写入 `k = 2`，master 执行后回复“成功”；
2. 这条写入还没发到 replica，master 就宕机了；
3. replica 被提升为新 master，它上面的 `k` 仍是旧值。

客户端已经收到“成功”，但这次写入永久丢失了。Redis 官方规范明确把这列为可能的故障模式。Redis 提供 `WAIT` 命令等待指定数量的 replica 收到写入，但官方文档同样说明，它不能让 Redis 成为强一致存储：故障切换时选出的 replica 不一定是收到了该写入的那个，写入仍可能丢失。([Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)；[WAIT](https://redis.io/docs/latest/commands/wait/))

### 3.2 无主 quorum：Cassandra / ScyllaDB

一个 partition 的多个副本地位对等，没有固定的 primary。coordinator 总是把写入发给**全部**副本，consistency level 决定它等待多少个副本确认后回复客户端：

- `ONE`：一个副本确认即返回，最快，但其他副本可能还没写入；
- `QUORUM`：多数副本确认，RF=3 时需要 2 个；
- `ALL`：全部副本确认，任一副本故障写入就失败。

读也同理。如果写入等待 W 个副本、读取询问 R 个副本，且 `W + R > RF`，那么读到的副本中至少有一个参与过写入，读就能看到这次写入。例如 RF=3 时读写都用 `QUORUM`：2 + 2 > 3。([Cassandra: Dynamo](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html)；[ScyllaDB: Fault Tolerance](https://docs.scylladb.com/manual/stable/architecture/architecture-fault-tolerance.html))

这种设计的好处是，单个副本故障时不需要先选出新 leader，剩下的副本只要够数就能继续服务；一致性级别也可以逐个请求调整。代价是副本之间会暂时不一致，需要额外机制处理：

- **冲突裁决：** 没有唯一的写入者，同一个键可能被并发写入不同的值。Cassandra 给每次写入附加时间戳，时间戳最新者胜出（last-write-wins）。这依赖各节点时钟大致同步，时钟偏差会导致“后写的被先写的覆盖”。
- **hinted handoff：** 写入时某个副本不在线，coordinator 先替它保存一条“提示”，等它恢复后补发。
- **read repair：** 读取时发现副本之间数据不一致，顺便把旧的副本修正。
- **anti-entropy repair：** 定期用 Merkle 树（一种按区间汇总哈希值的树，可以快速定位不一致的区间）比较副本，修复差异。

### 3.3 fencing：主从路线的关键难题

primary/replica 路线还有一个无主路线没有的问题：**脑裂**。假设旧 master 并没有宕机，只是和集群其他节点之间网络断了。集群认为它故障，提升了新 master；而旧 master 仍然可以被一部分客户端访问，继续接受写入。此时同一段数据有两个“权威”写入者，两边的写入必然有一边会丢失。

解决办法统称为 **fencing（隔离）**：确保旧 owner 在新 owner 生效前已经停止服务。常见手段：

- **epoch：** 每个请求和复制消息携带拓扑版本，节点拒绝来自旧 epoch 的指令；
- **租约（lease）：** owner 的权限有时限，必须定期向控制面续期；续不上就在租约到期时自动停止服务。控制面只要等旧租约确定过期，就可以安全地启用新 owner。

Redis Cluster 的做法是：master 与多数 master 失联超过 `NODE_TIMEOUT` 后就拒绝写入，以此限制少数派一侧能接受写入的时间窗口。

### 3.4 两条路线的对比

| | primary/replica | 无主 quorum |
| --- | --- | --- |
| 写入顺序 | 由单一 owner 决定，易于定义单调序号、请求去重和单键线性一致 | 多副本可并发写，需要时间戳和冲突裁决 |
| 单节点故障 | 需要检测故障、提升副本、隔离旧 owner，期间该分片短暂不可写 | 剩余副本够数时通常无需切换 |
| 数据丢失风险 | 异步确认时存在丢失已确认写入的窗口；可通过等待特定副本确认来缩小 | 取决于读写级别；`QUORUM` 读写可容忍少数副本故障 |
| 恢复 | 补齐副本数量 | 持续依赖 repair 让副本收敛 |
| 主要难点 | 提升哪个副本、如何隔离旧 owner | 副本如何收敛、冲突如何裁决 |

## 4. 调研结论

以上三个系统说明，分片方式、请求路由、拓扑管理和复制模型是几个可以分别选择的维度，不存在唯一的标准组合：

- Redis Cluster：固定 slot + 客户端路由 + gossip 管理拓扑 + 异步主从复制，数据路径最短，但拓扑协议、故障选举和客户端重定向都较复杂；
- Cassandra：token 环 + 节点转发 + gossip + 无主 quorum，单副本故障无需切换，但副本收敛和冲突裁决成为系统的重要组成部分；
- ScyllaDB：保留 Cassandra 的数据面，但用 tablets 和 Raft 管理数据分布与拓扑，说明控制面可以独立地走向集中排序。

结合本项目“客户端直连、在线扩缩容、一致性边界明确”的目标，设计选择如下：

1. **客户端路由。** 与 Redis Cluster 一样，客户端缓存完整拓扑并直接访问 owner，正常读写不多经过一跳。与 Redis 不同的是，数据分布保留一致性哈希和 vnode；拓扑下发的是完整的 token 分配，而不只是节点列表，客户端不必自行重建环，从而避免各客户端因实现或配置差异算出不同的环。
2. **独立协调器。** 参照 ScyllaDB 和 Cassandra 6.0 把拓扑变更集中排序的方向，由一个独立的 Coordinator 维护权威拓扑和 epoch，负责节点加入、退出、迁移切换和故障提升，但不参与正常读写。与 ScyllaDB 在数据节点间运行 Raft 不同，本项目用单个 Coordinator 实现，远比实现 gossip 或共识协议简单；代价是 Coordinator 自身的高可用不在本项目范围内。
3. **owner/follower 复制。** 与 Redis Cluster 一样，每个区间有唯一的 owner 决定写入顺序，这使序号、请求去重和迁移切换都容易定义。在此基础上，本项目用租约和 epoch 隔离旧 owner，并提供可配置的写入确认策略：默认只等 owner 确认，性能最好，但保留与 Redis 相同的异步丢失窗口；也可以要求“故障时将被提升的那个副本”先确认，这样单节点故障不会丢失已确认的写入。这一点与 Redis `WAIT` 不同：`WAIT` 只保证“某些副本”收到，而提升时未必选中它们。
4. **副本放置本身就是接管计划。** 这是本项目缩容与故障恢复机制的核心。一个区间的副本按环上顺时针依次选取不同的物理节点，排在第一位的 follower（first successor）恰好就是 owner 被移除后、环会自然选出的新 owner。因此，无论是计划内缩容还是节点故障，接管都不需要搬运大量数据：离开节点的区间直接并入下一个 owner 的管辖范围，而这个 owner 早已作为 follower 持有这部分数据。每个受影响区间的副本列表只是去掉离开的节点、其余节点依次前移一位，再在末尾补上一个节点；新 owner 可以立即用已有数据接管，副本的补齐在后台完成。与 Redis 需要选举、Cassandra 需要持续 repair 相比，接管者由环的结构直接决定，不需要选举，也不需要比较哪个副本更新。“恢复服务”和“恢复冗余”由此被分成两步：前者只涉及隔离旧 owner、验证数据覆盖和发布新拓扑，后者在后台完成。具体机制在“总体方案设计”一章展开，参考材料见 [缩容与故障接管](../rfcs/0001-scale-in-failover.md) 及其[交互演示](../rfcs/0001-scale-in-failover.html)。
5. **快照加增量迁移。** 与 Redis 8.4 原子迁移的思路相近：先开始记录增量，再复制快照并回放增量，最后对迁移中的区间短暂停写、校验数据一致后发布新 epoch；持有旧拓扑的请求会被告知拓扑已变并刷新重试。

没有选择 Cassandra 式无主 quorum，主要是因为本项目已经有客户端路由和区间 owner 的概念，保留单一 owner 能让写入顺序、迁移切换和一致性保证都更容易定义；而通过 follower、确认策略、租约和 epoch，又能缩小最简单的异步主从方案中的数据丢失窗口。

因此，一致性哈希只是数据分布的基础。本项目的设计重点是拓扑变化的管理，以及拓扑、数据迁移和副本权威三者在变化过程中如何保持一致，这将在“总体方案设计”一章展开。
