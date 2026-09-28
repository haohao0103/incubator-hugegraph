# Temporal 真实集群验证清单（脚手架 + 签收表）

状态：**真实集群执行中**（stg-121/122/123，2026-09-28 起；已完成滚动升级、flag 开启、
功能闭环与异常受控复查、故障恢复演练；性能/回滚演练与签收表待执行——实测记录、发现与
性能优化见第 10–11 节。本文档只记录真实执行结果，**不含、也不得填入伪造结果**）
关联：[TEMPORAL_GRAPH_SUPPORT_PLAN.md](TEMPORAL_GRAPH_SUPPORT_PLAN.md) §5.4 / §6.3 / §7.2 ·
[TEMPORAL_SERIALIZER_COMPATIBILITY_RFC.md](TEMPORAL_SERIALIZER_COMPATIBILITY_RFC.md) §7

## 0. 为什么这份清单是"待执行"而非"已通过"

设计文档 §5.4 / §6.3 / §7.2 反复冻结一条红线：**temporal 的性能门槛、滚动升级、回滚、
leader 切换、Raft replay、Store 重启、snapshot/restore、分片迁移必须在真实 HStore/PD/Raft
集群上验证；HBase / Memory / RocksDB 不作为 temporal 验收依据；真实压测前不得宣称达标。**

离线单测（`TemporalSuiteTest`，51 项）只能覆盖**不依赖活集群**的契约（编解码、幂等、
冲突仲裁逻辑、状态机 replay 分支、feature flag 门、fail-closed 版本拒绝）。下列每一项都
**需要一套真实运行的 3×PD / 3×Store / 3×Server 集群**，其结果只能由执行人在集群上采集后
填入第 8 节签收表，**不可由本次开发离线产出或推测**。

## 1. 集群脚手架（已就绪的真实产物）

| 产物 | 路径 | 用途 |
|---|---|---|
| HA 集群编排 | `docker/docker-compose-3pd-3store-3server.yml` | 3 PD + 3 Store + 3 Server，含健康检查、数据卷、端口映射 |
| Store 直连查询集成测试 | `hugegraph-store/hg-store-test/.../temporal/TemporalStoreDirectQueryTest.java` | append → as_of round-trip（经 PD 路由到 leader store 的真实 gRPC 路径） |
| Store 直连并发/TOCTOU 测试 | `hugegraph-store/hg-store-test/.../temporal/TemporalStoreDirectConcurrencyTest.java` | 重叠区间并发仲裁（恰一成功 + 一 `TEMPORAL_CONFLICT`）、同 `mutation_id` 重放 no-op；含 `main()` 可独立运行 |
| 离线回归套件 | `hugegraph-store/hg-store-test/.../temporal/TemporalSuiteTest.java` | 集群无关契约（升级/回滚前后都应保持全绿） |
| Store 启动脚本 | `hugegraph-store/hg-store-dist/.../bin/start-hugegraph-store.sh` | `-j` 注入 JVM 属性（feature flag 入口） |
| Store 容器入口 | `hugegraph-store/hg-store-dist/docker/docker-entrypoint.sh` | 把 `JAVA_OPTS` env 透传给启动脚本 `-j` |

### 1.1 集群端口（host 侧，来自 compose 映射）

| 角色 | gRPC | Raft | REST |
|---|---|---|---|
| pd0 / pd1 / pd2 | 8686 / 8687 / 8688 | 8610（内部） | 8620 / 8621 / 8622 |
| store0 / store1 / store2 | 8500 / 8501 / 8502 | 8510 / 8511 / 8512 | 8520 / 8521 / 8522 |
| server0 / server1 / server2 | — | — | 8080 / 8081 / 8082 |

集成测试默认连 `temporal.test.pd=127.0.0.1:8686`（pd0）、`temporal.test.graph=DEFAULT/hugegraph/g`，
可用 `-D` 覆盖。

### 1.2 启动 / 停止

```bash
cd docker
# 启动 HA 集群（镜像版本按需用 HUGEGRAPH_VERSION 指定）
HUGEGRAPH_VERSION=<tag> docker compose -f docker-compose-3pd-3store-3server.yml up -d --wait
docker compose -f docker-compose-3pd-3store-3server.yml ps
# 停止（保留数据）/ 彻底删除（含数据卷，谨慎）
docker compose -f docker-compose-3pd-3store-3server.yml stop
docker compose -f docker-compose-3pd-3store-3server.yml down       # 保留数据
docker compose -f docker-compose-3pd-3store-3server.yml down -v    # 删除数据（不可逆）
```

### 1.3 feature flag 注入机制（滚动升级关键）

`TemporalFeatureFlag` 在**类初始化时一次性**读取系统属性 `hugegraph.temporal.enabled`
（默认 `false`）。因此**开启 temporal 写必须以该属性重启 Store 节点**，不能运行时翻转：

```bash
# 对每个 store 服务设置 JAVA_OPTS 后重建/重启（entrypoint 经 -j 透传给启动脚本）
JAVA_OPTS=-Dhugegraph.temporal.enabled=true \
  docker compose -f docker-compose-3pd-3store-3server.yml up -d --force-recreate store0 store1 store2
```

flag off 时 temporal RPC 入口显式拒绝（`temporalMutation` → `RES_CODE_EXCESS`；
`addTemporalRaftTask` → `CLUSTER_NOT_READY`），**但已提交日志的 Raft apply / replay 不受
flag 影响**（回滚安全不变式，离线由 `TemporalFeatureFlagTest` pin）。

## 2. 七项性能门槛（§5.4 冻结值，须实测）

> 测量前置：集群健康（全 `healthy`）、flag 已开、预热完成。每项按下表方法采集，
> 结果填入第 8 节。**任一不达标即阻断对应 Phase / 发布（§6.5 失败处理）。**

| # | 指标 | 场景 / 规模 | 冻结门槛 | 测量方法 | 阻断 |
|---:|---|---|---:|---|---|
| 1 | 普通非时序查询 P95 回归 | 同一数据集，启用 temporal 前后对比 | P95 回归 ≤ 5% | 固定请求集，各跑 30 min，报告 P50/P95/P99 + 置信区间 | 发布 |
| 2 | `as_of` P95/P99 | 单实体、1000 条历史区间 | P95 ≤ 50 ms，P99 ≤ 100 ms | HStore/PD/Raft，预热后 10,000 次 | Phase 2/4 |
| 3 | `between`/`overlap` P95/P99 | 单实体 1000 条、多实体 100 条/实体 | P95 ≤ 100 ms，P99 ≤ 200 ms | 统计索引命中与扫描行数 | Phase 4 |
| 4 | append 吞吐 | 顺序/乱序/热点，1 KB mutation | ≥ 1,000 mutation/s，错误率 0 | 固定 16 并发、10 min 稳态 | Phase 2 |
| 5 | temporal 扫描行数 | 索引命中查询 | ≤ 返回行数 × 10 | 采集 Store/Region scan 指标（`scanned_rows` 已回传 REST） | 发布 |
| 6 | 当前/历史一致性 | 提交前、提交后、重启后 | 0 个不一致样本 | mutation 与双视图逐条对账 | Phase 2/3 |
| 7 | 资源放大 | 1 亿 interval、90 天保留 | 存储放大 ≤ 2.5×，compaction 放大 ≤ 3× | 固定保留周期与 mutation 大小 | Phase 6 |

**门槛 #1 的 A/B 纪律**：基线（无 temporal 改动）与 temporal 工作树须用**同一 JDK / Maven /
请求集**分别运行，记录 JDK 版本（§5.5 曾因 Java 26 工具链导致归因错误）。普通图回归差异
必须可归因，不得笼统计入 temporal。

**门槛 #5 的现成观测点**：Store 侧 `temporalQuery` 已统计 `scanned_rows` 并在扫描放大
> 10× 时 WARN；REST 响应回传 `scanned_rows`。压测时直接采集该指标与日志 WARN 计数。

## 3. 功能闭环（§6.3，真实集群）

用 Direct 集成测试 + REST 路径验证，全部经 PD 真实路由到 leader store：

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home
cd hugegraph-store/hg-store-test
CP="target/classes:$(cat target/cp.txt)"   # 含 store-core/common/grpc/client/node + struct + commons
# append -> as_of round-trip
"$JAVA_HOME/bin/java" -Dtemporal.test.pd=127.0.0.1:8686 -cp "$CP" \
  org.junit.runner.JUnitCore org.apache.hugegraph.store.temporal.TemporalStoreDirectQueryTest
# 并发 TOCTOU 冲突仲裁（20 轮，恰一成功 + 一冲突 + 重放 no-op）
"$JAVA_HOME/bin/java" -Dtemporal.test.pd=127.0.0.1:8686 -cp "$CP" \
  org.apache.hugegraph.store.temporal.TemporalStoreDirectConcurrencyTest
```

清单（逐项在集群上验证并记录）：

- [ ] append / upsert / close / delete 四操作经 Raft 多数派 apply 成功
- [ ] `as_of` / `between` / `overlap` 查询读回写入区间，边界（半开区间、相邻不冲突）正确
- [ ] 重叠区间并发提交：恰一成功 + 一 `TEMPORAL_CONFLICT`（无静默覆盖）
- [ ] 同 `mutation_id` 重放：no-op（OK）；payload 不同的重放：`IDEMPOTENCY_CONFLICT`
- [ ] 乱序写入、热点实体分桶不破坏区间序
- [ ] 共置组 `(graph, temporal_label, entity_id, fact_key)` 不跨 Region；跨 Region 请求 → `TEMPORAL_CROSS_REGION_UNSUPPORTED`
- [ ] 无界查询保护：超 `as_of.max_buckets/max_rows` → `TEMPORAL_QUERY_LIMIT_EXCEEDED`，返回实际回走桶数/扫描行数
- [ ] 普通非时序读写走原 HStore 路径，行为与启用前一致

## 4. 故障与恢复（§6.3 / §6.6，真实集群，不可离线替代）

| 场景 | 注入方法（示例） | 期望 |
|---|---|---|
| Store leader 切换 | `docker compose ... stop store<leader>` 或 kill 进程 | 新 leader 选出；切换后 temporal 读写恢复；已提交区间不丢、不重复 |
| Raft replay | leader 切换 / 重启后观察 follower 重放已提交日志 | 重放幂等（ledger 命中 no-op）；committedIndex 推进；temporal 字节流版本一致 |
| Store 重启 | `docker compose ... restart store<N>` | 重启后 current/history 双视图与重启前逐条一致（门槛 #6） |
| snapshot / restore | 触发 Store snapshot 后从 snapshot 恢复 | temporal 数据（同库 RocksDB checkpoint）随整库恢复；恢复后查询正确 |
| 分片迁移 | PD 触发 Region/partition 迁移 | 共置组整体迁移，不跨 Region 拆分同一事实序列 |
| 写入中断 + 重试 | 提交途中 kill leader | 客户端以原 `mutation_id` 重试 → 幂等，无重复区间 |
| 滚动升级 | 见第 5 节 | 混合版本期 temporal 写不上线；全量升级后开 flag 正常 |
| 回滚 | 见第 6 节 | 普通图可用；temporal 显式不可用 |

> replay / leader 切换的**离线**等价契约已由 `PartitionStateMachineTemporalReplayTest`
> （replay 分支日志推进、未注册 op 显式失败、业务拒绝分类）覆盖；但上表的**真实**切换、
> 重启、snapshot-restore round-trip 必须在集群上跑，离线测试不能替代。

## 5. 滚动升级演练（§6.6，与 Serializer RFC §5 升级路径对应）

步骤（每步记录节点版本与 flag 状态）：

1. [ ] 起点：3 Store 均为**旧版本**（不支持 temporal），flag off。普通图读写正常。
2. [ ] 逐个升级 Store 到**新版本**（flag 仍 off）：`up -d --force-recreate store0` → 等 healthy → store1 → store2。
   - 期间验证：普通图读写不中断；temporal RPC 被拒（flag off）；**无 temporal 写进入 Raft**。
3. [ ] 升级 3 Server 到新版本，确认 capability 握手成功（`SCHEMA_VERSION` 一致）。
4. [ ] 确认**所有** Store/Server 版本一致（共享同一 `CODEC_VERSION=3` / `MARKER_VERSION=1` / `SCHEMA_VERSION=1`）。
5. [ ] 全量开 flag：对 store0/1/2 设 `JAVA_OPTS=-Dhugegraph.temporal.enabled=true` 并 `--force-recreate`（见 §1.3）。
6. [ ] 开 flag 后跑第 3 节功能闭环 + 第 2 节门槛抽测，确认正常。

**升级期不变式**：混合版本阶段 temporal 写**永不上 Raft 线**（flag off），故不存在
"v3 leader 编码被 v2 follower 拒绝"的在线分歧；fail-closed codec 只在违反升级纪律或数据
损坏时触发（Serializer RFC §3）。

## 6. 回滚演练（§5.1 回滚语义 / §6.6）

1. [ ] 关 flag 回滚：以 `JAVA_OPTS` 不含该属性（或 `=false`）重启 Store。
   - 验证：新 temporal 写被拒；**已提交日志仍能 replay**（flag 不拦 apply/replay）；普通图正常。
2. [ ] 降级到不支持 temporal 的旧版本（如需）：
   - 验证：普通图继续可用；temporal 数据**不得**按普通 vertex/edge/property 解读；
     capability 路径显式返回 `TEMPORAL_UNSUPPORTED_VERSION`；有显式告警。
3. [ ] 回滚后跑离线 `TemporalSuiteTest`（应仍 51 项全绿）+ 普通图全量回归。

## 7. 合并前硬门槛核对（§7.2）

- [ ] 所有新增 temporal 单元测试通过（离线 `TemporalSuiteTest` 51 项）
- [ ] HStore/PD/Raft 是唯一 temporal backend 目标；未以 HBase/Memory/RocksDB 测试替代
- [ ] 现有非时序 core/API/TinkerPop 测试全量通过
- [ ] 旧数据无需迁移即可继续普通查询
- [ ] 真实集群 append/query/as_of/between/overlap/close、幂等、重启恢复、Raft replay 通过（第 3/4 节）
- [ ] 无静默全表扫描、无无界历史返回（门槛 #5）
- [ ] 有明确 feature flag、回滚后 temporal 数据不可用的显式告警、数据保留方案
- [ ] Java/Go client 范围结论 + REST 契约文档齐全
- [ ] hugegraph-computer/OLAP 范围外、瞬时 event 范围外声明已记录
- [ ] 无 OBKV 代码/依赖/backend
- [ ] 工作树与提交历史可审计，测试日志写入项目 `_out` 目录

## 8. 签收表（执行人在真实集群采集后填写；开发阶段留空）

> **纪律**：本表只能由实际运行集群的人填入真实测量值与日志路径。开发/离线阶段保持空白，
> 不得预填、估算或伪造。任一项空白即表示该门槛"待真实集群验证"，不得宣称达标。

| 项 | 执行人 | 集群版本/JDK | 实测结果 | 日志/证据路径（`_out/...`） | 通过? |
|---|---|---|---|---|---|
| 门槛 #1 普通查询 P95 回归 | | | | | ☐ |
| 门槛 #2 `as_of` P95/P99 | | | | | ☐ |
| 门槛 #3 `between`/`overlap` P95/P99 | | | | | ☐ |
| 门槛 #4 append 吞吐 | | | | | ☐ |
| 门槛 #5 扫描行数 ≤ 10× | | | | | ☐ |
| 门槛 #6 当前/历史一致性 | | | | | ☐ |
| 门槛 #7 资源放大 | | | | | ☐ |
| 功能闭环（第 3 节） | | | | | ☐ |
| 故障恢复（第 4 节） | | | | | ☐ |
| 滚动升级（第 5 节） | | | | | ☐ |
| 回滚演练（第 6 节） | | | | | ☐ |
| 合并硬门槛（第 7 节） | | | | | ☐ |

## 9. 已知缺口（诚实登记，不投机自建）

- **自动数据保留 / 清理**：当前无代码。门槛 #7 的"90 天保留"需要保留策略实现后才能端到端
  验证；审计留存 vs 清理的取舍需独立设计（见 Serializer RFC §7 延后项），不在本清单内臆造。
- **压测驱动脚本**：第 2 节门槛 #2/#3/#4 需要 10,000 次 / 16 并发 / 10 min 稳态的压测驱动。
  现有 Direct 测试是**功能/仲裁**入口，非高并发压测 harness；压测驱动须按目标规模另行搭建
  （可复用 Direct 测试的 PD 路由 + gRPC 提交路径），其产出结果同样只能实测、不可伪造。

## 10. 执行记录：stg 集群实测（2026-09-28，执行中）

> **纪律**：以下均为 stg-121/122/123 真实集群实测记录（含受控复测与代码级根因定位）；
> 未完成项如实标注"待执行"，不得视为已通过。

### 10.1 环境与部署

| 项 | 值 |
|---|---|
| 节点 | stg-121 = 10.129.33.121、stg-122 = 10.129.33.122、stg-123 = 10.129.33.123 |
| 部署 | 3×Store + 3×Server（PD 沿用现网，无需升降级），部署目录 `/home/data/huge-dis/` |
| Store 端口 | gRPC 8550 / Raft 8510 / REST 8520；Server REST 8080 |
| 分支 | `feature/temporal-graph-enhance`（含 master 合并），构建含 temporal 的 Store/Server 部署包 |

证据：本地 `target/*.sh` 执行脚本；远端 `/tmp/stg-*.log` 与会话即时采集输出。

### 10.2 已完成项（真实执行）

1. **滚动升级**：3 Store 逐个升级（flag off，先备份现部署），期间普通图读写不中断；随后 3 Server 逐个升级。
   脚本：`target/upgrade-hg-store.sh`、`target/upgrade-hg-server-v2.sh`。
2. **开 flag**：3 Store 设 `-Dhugegraph.temporal.enabled=true` 重启（`target/switch-flag-on.sh`）。
3. **功能闭环（部分）**：
   - REST 补充测试 16 子项（append/upsert/close/delete、as_of/between/overlap、重放、分页、回归）：
     14 项直接通过；2 项（close、重放改 payload）出现 500——经受控复测归因为"发现 1"的受害者
     （见 10.3），**非** close/replay 语义缺陷。脚本：`target/stg-rest-supplement.sh`。
   - Store Direct 集成（append → as_of round-trip，经 PD 真实路由到 leader store）。
4. **异常受控复查（R1-R11 静默复测**，独立 fact key `{"x":"rep1"}` / entity `stg-ts-2`，脚本 `target/stg-close-verify.sh`）：
   - R1/R2 append 成功（index 1386/1387 committed）；R9 再次 close 成功
     （index 1389 CLOSE committed views=0）→ **close 语义完好**；
   - R6 改 payload 重放 → 200（index 1388 ledger-hit no-op）→ **幂等去重生效**；
   - R10/R11：对不存在区间 close/delete → 原生错误文本
     `"no interval to close at ..."` / `"no interval to delete at ..."`；
   - R3/R7/R8：收到 `"TEMPORAL_CONFLICT for fact key"`（append 专属文本）却来自 close/replay 请求，
     且自身无任何 apply entry——**与其操作不匹配**；
   - 三对一一对应：R3 500 ↔ 同刻 `stg-flag-off-2@1380 rejected`；R7 500 ↔ `m6@1760 rejected`；
     R8 500 ↔ `m7@1761 rejected`（3/3 无例外）。
5. **故障恢复演练（cv-fault，见 10.6）**：污染判别（Server 重启前后对照）、Store 重启 + Raft replay 幂等、
   leader 切换 + 写入中断重试幂等，全部实测通过。

### 10.3 发现 1：pendingTemporalWrites 残留引发跨请求"幽灵重放 + 响应错配"（Server 层缺陷，根因已定位）

**现象**：某 temporal 写请求失败（如区间冲突）后，**同一 HTTP worker 线程**上后续的 temporal 请求会：
(a) 收到**上一个失败请求**的错误消息（响应错配，误导排障）；(b) 自身 bundle 从未提交（无 apply entry）；
(c) 同时刻日志出现更早的失败请求以**新 raft index** 被再次 propose 并再次被拒（幽灵重放，日志噪声）。

**判别证据**：R10/R11 证明 close/delete 原生文本与 `"for fact key"` 不同 → R3/R7/R8 的 500
不可能源于自身操作；R9/R6 证明语义本身完好。历史幽灵（stg-func-1/2/3、flag-off-1/2、mutation-a/b、
m4、m6、m7）与测试请求的"同时刻"对应同源。

**根因（代码级，已逐环节核实）**：
1. `GraphTransaction.pendingTemporalWrites`（实例字段）：`flushTemporalOnly()` /
   `flushPendingTemporalWrites()` 循环 dispatch 时任一请求被 Store 拒绝（如 `TEMPORAL_CONFLICT`）
   → 异常中断 → **`clear()` 不执行 → 请求残留**；
2. `GraphTransaction` 实例被 `StandardHugeGraph.TinkerPopTransaction` 的 `ThreadLocal<Txs>`
   **按线程长期复用**（`setClosed()` 仅置标志位、`getOrNewTransaction()` 注释明确 "for reusing backend tx"；
   `destroyTransaction()` 仅在 graph/进程关闭时调用）；
3. 后续请求（同线程）flush 时**先重提残留请求** → 残留再次被拒 → 异常直接冒泡给当前请求
   （消息=残留请求的错误），当前请求 bundle 从未 dispatch；
4. `GraphTransaction.rollback()` 与 `API.commit()` 的异常回滚路径**均不清理** `pendingTemporalWrites`
   → 污染在进程存活期内**无限持续**（残留链中只要有"确定性冲突"请求，该线程永久污染；
   无确定性冲突的残留链可在某次 flush 全部成功后自愈，解释了多数历史幽灵后来消失）。

**影响**：被污染线程上的 temporal 请求持续失败且报出错误的原因；写请求静默丢弃（客户端 500 已感知）；
无数据损坏（幽灵重提是合法新 propose，被正确拒绝）。**范围**：Server 端事务层，与 Store/Raft 无关。

**复现**：确定性复现——制造一个确定性冲突请求 A 使其失败，随后从同一 worker 发正常请求 B
→ B 也 500 且原因为 A 的；**预测已端到端确证**：重启 Server 后污染消失（见 10.6-A：
重启前 30 探测 11 失败 → 重启后同一探测 30/30 成功且幽灵计数冻结）。

**修复方向（待评审）**：flush 循环逐请求隔离（失败即记录并移除，不中断其余），以及 rollback/close 时
清空 `pendingTemporalWrites`；原则：temporal 失败是请求级结果，不得污染 tx 的后续复用。

### 10.4 发现 2：重放指纹比对未实现（实现落后于设计文档）

- 设计（§5.3 冻结契约，`TEMPORAL_GRAPH_SUPPORT_PLAN.md`）：同一 `mutation_id` 重放时
  canonical fact key / schema_version / valid interval / payload hash **任一不同必须 `IDEMPOTENCY_CONFLICT`**。
- 实测：R6 改 payload 重放 → **200 no-op**（Store 端 ledger 命中即 no-op，无指纹比较；
  代码 `TemporalMutationHandler.apply` 的 ledger 检查先于 contributeOp 执行）。
- 影响：无法识别"同 mutation_id 不同内容"的客户端事故（幂等保护弱于设计）；无数据损坏（首次提交语义生效）。
- 处置：登记为待修复项（Store 端 ledger 记录指纹并在重放时比对）。

### 10.5 待执行

- ~~故障恢复（第 4 节）~~：已完成核心四项（见 10.6）；**snapshot/restore round-trip 与分片迁移未做**，登记待补；
- 性能门槛七项（第 2 节）：需先搭建压测驱动（§9 已登记缺口）；
- 回滚演练（第 6 节）；
- 第 8 节签收表：待上述完成后填入实测值。

### 10.6 故障恢复演练实测（cv-fault，2026-09-28 下午）

**A. 污染判别验证（对应 10.3 预测；脚本 `target/stg-fault-probe.sh`）**

- 重启前：30 个 no-op 重放探测 → **19 OK + 11 个 500**；幽灵以**新 raft index** 重现
  （flag-off-2@1382/1384、m6@1762/1764、m7@1763），计数 6/4/4 → **10/8/6**。
- 重启 121 Server（进程 13:38:47 → 14:38 换新 pid 27261）后同一探测：**30 OK + 0 BAD**，
  幽灵计数**冻结于 10/8/6**（无任何新 rejected）。
- **结论：10.3 根因（线程级 tx 残留）端到端确证**；R3/R7/R8 相同参数在干净进程下全部 200。

**B. Store 重启 + Raft replay 幂等（stg-123）**

- 停服：优雅停 30s 超时→强杀（已知 ContextClosedListener 等待缺陷，升级脚本含回退处理）；
  启动新 pid 26554。
- 启动 replay 新日志 1009 行：temporal apply entry **69 次**，其中 **ledger-hit no-op 53 次**
  （重放幂等的直接证据）；10 次历史冲突决策重放（rejected 条目的决策重放，无副作用）。
- 数据一致性：重启前后 between/as_of 查询**逐字节一致**；no-op 重放 200；123 恢复服务。

**C. Leader 切换 + 写入中断重试（Raft 4 = 事实键 `{"x":"lf1"}`，leader=121 store）**

- 基线写 `lf-1` → 200；`kill -9` store-121（14:49:22）。
- 中断窗口写 `lf-2` → **500 `UNAVAILABLE: io exception`**（客户端仍指向死节点）。
- **3 秒完成选举**：14:49:25 122 `Raft 4 becomes leader`（term 5→6），123 转向 following 122:8510。
- 同 mutation_id 重试 → **4 秒时第 1 次重试即 200**。
- 121 重启（pid 28777）后作为 follower 重新加入（14:49:34 following leaderId=122:8510 term=6）。
- **最终对账（between 全量）**：`lf-1`/`lf-2`/`lf-3` 三条区间无缝连续（rev 1446/1448/1449），
  **零重复、零丢失**；恢复后新写 `lf-3` → 200。

**工具链备注（非产品缺陷）**：121 Server 绑定节点 IP（10.129.33.121），本机 `127.0.0.1` 探测会被拒，
验证脚本应以节点 IP 探测；Store 优雅停 30s 超时需强杀回退（上文缺陷）。

## 11. 性能审计与优化（2026-09-28，代码级；待部署 + cv-perf 实测）

> **纪律**：本节记录代码级审计结论、优化实现与离线验证状态；所有端到端性能变化**只能**由
> 重新部署后的第 2 节七项门槛实测（cv-perf）确认，本节不含推测数值、不宣称达标。
> **部署状态**：本节全部改动（S1–S12 与 §11.4 修复）为本地实现，**尚未部署到 stg**，现网仍是
> 优化前构建；重部署须走第 5 节滚动升级纪律。

### 11.1 审计范围与方法

对 temporal 全链路 20+ 文件逐调用点做成本分解（每次写/读路径的 JCA 调用、堆分配、seek 次数、
锁竞争面）：Server 侧 `TieBreakers` / `TemporalRowKeyCodec` / `TemporalFactKey` /
`TemporalMutationPlanner` / `HstoreStore`；Store 侧 `TemporalIntervalCodec` /
`TemporalMutationHandler` / `TemporalQueryHandler` / `TemporalMutationBundleCodec`。
所有优化以**不改变任何线上字节布局**为前提（bundle codec v3 / marker layout v1 / row-key 编码与
tie-breaker 字节全部不变，由 `TemporalSerializerCompatibilityTest` 与既有字节断言守门）。

### 11.2 修复清单（S1–S12，已实现；离线回归全绿）

| # | 位置 | 原成本 | 优化 |
|---|---|---|---|
| S1 | Server `TieBreakers.sha256` | 每次调用 `MessageDigest.getInstance("SHA-256")`（每写多触发：tie_breaker + fact-key hash + colocation hash） | `ThreadLocal<MessageDigest>` 池化 |
| S2 | Server `TemporalMutationPlanner.plan` | 每 view 重取 canonical 字节 + 重算 history row key 前缀 | fact key 读一次 + 行键复用已算 group prefix |
| S3 | Server `TieBreakers.concat` | `ByteArrayOutputStream` 多轮扩容复制 | 两遍法预分配（字节输出不变：4B 大端长度 + payload） |
| S4 | Server `TemporalFactKey` | 包内热路径经 `canonicalBytes()` 反复防御性拷贝 | 包内 `canonicalBytesView()` 无拷贝视图（外部契约不变） |
| S5 | Server `HstoreStore` | 每请求 `new TemporalRowKeyCodec()` | `static final` 单例（无状态不可变） |
| S6 | Store `TemporalMutationHandler.apply` | 热路径内含 `getSession(groupId)` instrumentation 块 + 逐行 debug `toHex` | 移除热路径 instrumentation 与逐行日志 |
| S7 | Store 写锁 | `ConcurrentMap` 无界 per-fact intern 锁（长生命周期 Store 上的慢泄漏 + 全局 map 竞争） | 1024 条定长条带锁（同 fact 恒定同条带，互斥性不变） |
| S8 | Store `hasConflict` | 1 次 fact 前缀全扫描：逐行 parse 全部历史（含每行 debug） | 三段剪枝 + 首行探测快路径 + legacy 兜底（见下表） |
| S9 | Store `findInterval` | 1 次 fact 前缀全扫描定位单区间 | 两次单桶精确 seek：先 `OPEN_BUCKET`（保持原 key 序）再 `bucketOf(validFrom)` 桶 |
| S10 | Store `TemporalMutationBundleCodec.decode` | Raft 载荷整体拷贝切片后再解码 | `(byte[], offset, length)` 原地解码重载 |
| S11 | Store `TemporalIntervalCodec` | `intervalKey`/`bucketPrefix` 经 BAOS 扩容 | 预分配数组直写（字节不变） |
| S12 | Store `TemporalQueryHandler.scanBucket` | debug 参数字符串（含 factKey UTF-8 解码）无条件拼接 | `isDebugEnabled()` 门后构造 |

**S8 成本对照（append 冲突检查的 seek/行解析量）**：

| 场景 | 旧 | 新 |
|---|---|---|
| 全新 fact（无历史行） | 1 seek + 0 行 | 3 个单桶 seek |
| 补录（所有行 ≥ 候选桶） | 1 seek + N 行 parse | 3 个单桶 seek + 0 行 parse |
| 历史 fact（早桶命中，跨桶距 D ≤ 512） | 1 seek + N 行 parse | 3 + D 个单桶 seek，命中即决断 |
| 极老数据（早桶距 > 512，≈10 年+） | 1 seek + N 行 parse | legacy 全前缀兜底（正确性优先，罕见） |

正确性论证（已落盘代码注释）：早桶 ACTIVE 行在非重叠不变式下 `a.to ≤ newFrom`，任意更早行
`b.to ≤ a.from < newFrom` → 不跨越候选区间即可安全早停；低桶 PAST 行在桶前提下不可能出现
（防御性 legacy 回退）；范围扫描右界以 `bucketOf(candidateTo-1)+1` 精确保界；覆盖判定恒用完整公式
`newFrom < existingTo && from < candidateTo`（不化简，零长度边界行为与旧实现一致）。

### 11.3 离线验证（完成）

- Store 侧 `TemporalSuiteTest`：**51/51 全绿**（含 `TemporalSerializerCompatibilityTest`，字节兼容不回退）；
- Server 侧 9 个 temporal 测试类：**48/48 全绿**（tie-breaker base32 值、row-key 宽度 94B 等既有字节断言均未变）；
- 两侧全量编译通过；mock 测试同步补齐了新 scan 调用面的 stub（语义与真实 Store 的 `[start, end)` 扫描一致）。

### 11.4 附带发现：`BinaryEntryIterator` “泄漏 WARN”为计数误报（根因已定位并修复）

**现象**：滚动升级完成起（stg-121 于 13:38:59），三台 Server 持续输出
`BinaryEntryIterator cleaned without explicit close (... occurrences total)`，当日下午每台累计约
1200 条 WARN（全局计数器约 12 万次）。该日志来自 master 合并带入的 Cleaner 安全网（上游提交
`ea06593ab`/`fc5fec6c0`）。

**判别证据**：对 stg-121 全量 WARN 按线程名归类——约 1200 条中 1191 条来自 `task-db-worker-1`，
其余来自 `grizzly-http-server-*`，**0 条来自 Cleaner 线程**，且计数随查询/任务吞吐匀速增长
→ 全部“泄漏”都发生在业务线程的同步路径上，不存在任何 GC 触发的真实泄漏。

**根因（代码级）**：`Cleaner.Cleanable.clean()` 的规范语义是**在调用线程同步运行** cleaning
action；`doClose()` 调用 `cleanable.clean()` 因此同步执行了 `CleaningState.run()`，而 run() 无条件
`CLEANED_COUNT.incrementAndGet()`——**每次正常显式 close 都被计成一次“泄漏”**。设计意图
（代码注释 “eager close() paths should make this fire only for genuinely abandoned iterators”）与实现不符。

**修复**：`CleaningState` 增加 `explicitlyClosed` 标志；`doClose()` 先 `markExplicitClose()` 再
`clean()`——run() 仅在 **GC 触发**时计数 + WARN；显式 close 只关资源、不计数。新增 2 项回归测试
（显式 close / 耗尽 eager close 不计数），`BinaryEntryIteratorTest` **5/5 全绿**。

**影响与处置**：底层迭代器实际**始终被正确关闭**（显式路径经 clean() 同步关闭，弃用路径经 GC 兜底），
本缺陷纯属计数与日志污染（WARN 洪泛 + 每秒级日志 I/O）。现网 stg 为修复前构建，洪泛在重部署前持续；
重部署后若再出现该 WARN，**先看线程名：`Cleaner-*` 才代表真实泄漏路径**。

### 11.5 待办（与第 2/8 节联动）

- 重新构建含本节全部改动的 Store/Server 包并部署（下次窗口，走第 5 节滚动升级纪律）；
- 部署后执行 cv-perf：七项性能门槛实测并回填第 8 节签收表（**唯一**的性能达标依据）；
- 部署后观察 24h：`BinaryEntryIterator` WARN 应归零（或仅剩 `Cleaner-*` 线程条目）。

