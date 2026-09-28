# Temporal 真实集群验证清单（脚手架 + 签收表）

状态：**待真实集群执行**（本文档只备齐脚手架与清单，**不含、也不得填入伪造结果**）
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
