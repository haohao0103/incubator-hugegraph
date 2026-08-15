# HugeGraph 时序图谱支持开发与验证计划

## 1. 文档目标

本文档定义在 `incubator-hugegraph` 中新增时序图谱能力的开发边界、架构方案、兼容性约束、实施阶段、测试矩阵和验收标准。

开发分支：`feature/temporal-graph-support`
基线提交：`ea06593ab`

核心目标：新增时序图谱能力，同时保证现有非时序图谱的写入、查询、序列化、索引、后端和 API 链路默认行为不变。

## 2. 设计契约

### 2.1 时间维度

第一阶段只实现 Valid Time；Transaction Time 仅预留字段和版本扩展点，不在本阶段提供查询语义。

| 时间维度 | 含义 | 第一阶段状态 |
|---|---|---|
| Valid Time | 事实在现实业务中成立的时间 | 实现 |
| Transaction Time | 事实进入系统的时间 | 仅预留 |

时间输入支持 ISO-8601 UTC 字符串和 epoch millis；响应统一返回 ISO-8601 UTC，可选返回 epoch millis。时间粒度必须显式记录为 `DAY`、`SECOND`、`MILLIS` 等，不能把只有年份的数据伪装成精确日期。`Duration`、TTL 和业务有效区间不得混用。

### 2.2 区间边界

所有有效区间统一使用半开区间 `[valid_from, valid_to)`：

- `valid_to = null` 表示仍然有效；
- `valid_from < valid_to`，`valid_from == valid_to` 非法；
- `as_of(t)` 返回满足 `valid_from <= t` 且 `t < valid_to` 的区间；
- `between(from, to)` 返回与查询区间有交集的区间；
- `overlap` 只按半开区间相交规则判断。

### 2.3 共存原则

普通图谱与时序图谱共享图引擎、权限体系和事务基础设施，但必须隔离：

1. 普通 Vertex、Edge、PropertyKey、IndexLabel 和 `DataType.DATE` 继续保持原语义；
2. 时序事实使用独立的 `TemporalSchema`、`TemporalInterval`、`TemporalMutation` 和 `TemporalQuery` 模型；
3. 普通属性 `property(key, value)` 不升级为时序写入，不改变覆盖语义；
4. 未带 temporal 条件的普通查询继续走现有路径，不隐式返回历史版本；
5. temporal 查询必须显式使用独立 REST 资源或明确 temporal API，不修改标准 Gremlin `has()` 的默认含义；
6. 普通存储布局与 temporal 存储布局隔离，禁止用普通 Vertex/Edge 解析 temporal 专用数据。

### 2.4 区间事实模型

第一阶段采用区间事实模型，不采用“按单点追加”的时间序列模型：

1. `TemporalInterval` 表示一条带 `valid_from/valid_to` 的原子事实版本；
2. 时序事实通过独立追加/修订接口写入，不复用普通 property 覆盖语义；
3. 同一实体、同一 temporal label、同一事实键的有效区间不得重叠；重叠写入返回明确冲突错误，不采用隐式 last-write-wins；
4. `append` 创建新区间；
5. `upsert` 在同一事务内关闭受影响的开放区间并创建新区间；
6. `close` 只允许关闭开放区间，`close_time` 成为 `valid_to`；
7. `delete` 写入可审计的删除/失效 mutation；
8. 已关闭区间拒绝一切写入和原地修改；历史订正不在本阶段提供，必须另立订正流程，禁止把冲突错误当成普通更新失败来绕过；
9. 重复 mutation 必须通过幂等键处理，乱序写入、重试和重复提交语义在 Phase 0 冻结；
10. 瞬时事件不属于本阶段区间模型，独立 event 语义也不在本阶段实现，后续另立 RFC；
11. 旧数据没有时态字段时，只进入当前视图，不虚构历史时间。

### 2.5 事务与视图一致性

`append`、`upsert`、`close`、`delete` 由同一个 `GraphTransaction` 原子提交普通 mutation 与 temporal mutation：

- 提交前，当前视图和历史视图均不可见；
- 提交后，两者同时可见；
- 禁止异步双写作为第一阶段一致性方案；
- `TemporalBackendStore` 由 `GraphTransaction` 协调，HStore/PD/Raft 负责复制、提交和 replay；
- capability 不支持 temporal 时必须返回明确的 not-supported 错误，不得静默全表扫描或退化为普通属性读取。

### 2.6 后端与回滚

第一阶段只以 HStore/PD/Raft 为 temporal backend 目标，不实现 HBase、Memory 或 RocksDB temporal 适配。普通图的既有 backend 行为不受影响。

回滚到不支持 temporal 的版本后：

- 普通图数据继续可读写；
- temporal schema 和 temporal 数据不得被解释成普通 vertex、edge 或 property；
- 版本必须显式报告 temporal 数据不可用；
- 禁止静默丢失、伪造读取或自动转换 temporal 数据。

## 3. 代码扩展点

### 3.1 核心引擎

- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/backend/store/BackendStore.java`
- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/backend/tx/GraphTransaction.java`
- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/structure/HugeVertex.java`
- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/structure/HugeEdge.java`
- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/schema/PropertyKey.java`
- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/schema/IndexLabel.java`

### 3.2 类型与序列化

- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/type/define/DataType.java`
- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/backend/serializer/BinarySerializer.java`
- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/backend/store/raft/StoreSerializer.java`
- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/io/GraphSONSchemaSerializer.java`

现有 `DataType.DATE` 编码必须保持不变；新增时序编码只能追加，不能复用或重编号既有 enum/code。

### 3.3 API 与查询

- `incubator-hugegraph/hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/`
- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/backend/query/ConditionQuery.java`
- `incubator-hugegraph/hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/traversal/optimize/HugeVertexStep.java`

第一阶段优先新增独立 temporal REST 资源；路径使用 `/intervals` 或 `/facts`，不使用会暗示点模型的 `/points`。

## 4. 兼容性红线

1. 不改变普通 Vertex/Edge 的 ID 生成规则；时间不能默认进入主键或 Edge sort key；
2. 不把单值 Date 属性隐式升级为 list/set，也不改变普通 property update 的覆盖语义；
3. 不修改旧 API 的默认请求和响应结构；temporal 字段全部可选；
4. 不修改普通查询的执行计划；没有 temporal 条件时必须走原路径；
5. 不给 `BackendStore` 增加无默认实现的抽象方法；优先使用 `TemporalBackendStore` 子接口或 capability；
6. 不把 temporal TTL 与现有 Edge TTL 混用；
7. 不允许不支持 temporal 的 backend 静默退化为全表扫描；
8. 新增 schema、索引、Raft/gRPC、snapshot/restore 字段必须可选、可版本化、可回放；
9. 旧数据没有时间信息时不得伪造精确历史时间；
10. 不保留、不新增、不混用 OBKV；本阶段 temporal 底层只考虑 HStore/PD/Raft，不考虑 HBase、Memory 或 RocksDB temporal backend。

## 5. Phase 0 契约冻结记录（审计整改）

本节是对执行审计的整改记录。Phase 1 代码不得替代 Phase 0 契约材料；未标记为“已冻结”的条目仍是后续实现阻断项。

### 5.1 八项契约裁定

| 契约项 | 冻结裁定 | 状态 |
|---|---|---|
| RFC/对象模型 | `TemporalInterval` 只携带 `entity_id`、`temporal_label`、`fact_key`、有效区间和粒度；实体标签由外层实体路径与 `TemporalSchema` 绑定，不冗余进入每条历史行。 | 已冻结 |
| 区间状态机 | `append`：不存在重叠区间时创建；`upsert`：同事务关闭受影响开放区间并创建新区间；`close`：只关闭开放区间；`delete`：写入可审计失效 mutation；已关闭区间不可原地修改。 | 已冻结 |
| 冲突规则 | 同实体、同 temporal label、同 fact key 的半开区间发生交集即拒绝；相邻 `[a,b)` 与 `[b,c)` 不冲突；重复 mutation 必须以相同幂等键去重，幂等键复用但 payload 不同必须拒绝。 | 已冻结 |
| 事务/视图一致性 | 普通 mutation 与 temporal mutation 由同一 `GraphTransaction` 原子提交；提交前当前/历史视图都不可见，提交后同时可见；禁止异步双写。 | 已冻结 |
| 回滚语义 | 回退到不支持 temporal 的版本时，普通图继续可用；temporal 数据不得按普通 vertex/edge/property 解读，必须显式返回不支持。 | 已冻结 |
| 客户端范围 | Phase 0/1 只冻结 REST 契约评估；Java/Go client 不新增 SDK 行为，待 REST DTO、错误码和分页 token 稳定后另立任务。 | 已冻结 |
| OLAP 范围外 | hugegraph-computer/OLAP 不感知 temporal，不提供 temporal join、时间快照计算或 temporal traversal；这些需求另立 RFC。 | 已冻结 |
| Event 范围外与性能 | 瞬时 event 不纳入 interval 模型，另立 RFC。性能目标按下表冻结；真实 HStore/PD/Raft 压测前不得宣称达标。 | 已冻结 |

### 5.2 身份归属与序列化裁定

- `TemporalInterval` 的实体归属由 REST 路径/存储上下文提供，`entity_id` 仍作为 interval 的业务键组成部分；interval 不复制 `entity_label`。
- `TemporalSchema.entity_label` 用于校验外层实体 label 与 schema 的绑定；`TemporalSchema.check(interval)` 只校验 temporal label 和 fact key，实体 label 校验由 schema 绑定入口完成。
- 普通图旧对象和旧序列化编码不得改变。Phase 1 当前只冻结 temporal core 对象的 UTC/epoch/ISO 毫秒精度与明确时区拒绝规则；旧 schema 反序列化/新旧 schema 互读不在本阶段实现，改由后续“Temporal Serializer Compatibility RFC”单独定义 payload 版本、schema copy/round-trip、旧 payload 显式拒绝和升级路径。
- 旧数据没有时态字段时只进入 current-only 视图，不补造 `valid_from`、`valid_to` 或精确粒度。

### 5.3 Phase 0 错误码与 mutation_id 契约

以下错误码属于 Phase 0 冻结的 REST/Store 契约，设计文档不得新增未登记的同义错误码：

| 错误码 | 触发条件 | 客户端/服务语义 |
|---|---|---|
| `IDEMPOTENCY_CONFLICT` | 同一 `mutation_id` 再次提交时，canonical fact key、区间或 payload 任一不同 | 拒绝写入；原 mutation 的结果不可被覆盖。`mutation_id` 由客户端生成并在首次提交后持久化，重试必须携带原值。 |
| `TEMPORAL_CONFLICT` | 同事实键半开区间发生相交冲突 | 拒绝写入；客户端仅能以原 `mutation_id` 重试；不得静默覆盖或改写冲突区间。 |
| `TEMPORAL_CROSS_REGION_UNSUPPORTED` | 同一事实序列的 current/history/open-index/index placement 不共置，或一次 temporal mutation 触及多个 Region | 拒绝整个 mutation，不允许部分提交、跨 Region 异步补写或静默降级；返回 placement 诊断信息。 |
| `TEMPORAL_QUERY_LIMIT_EXCEEDED` | `as_of` 回走超过 `as_of.max_buckets` 或 `as_of.max_rows` | 拒绝无界扫描，返回实际回走桶数/扫描行数和配置上限；客户端必须缩小查询范围或显式使用受支持的分页/限制。 |
| `TEMPORAL_UNSUPPORTED_VERSION` | 当前 Server/Store 不支持 temporal schema、row-key 或 capability version | 只拒绝 temporal REST/Gremlin capability 路径；普通图继续读写，不将整个 graph 服务置为只读。 |
| `UNKNOWN_TEMPORAL_SCHEMA` | schema/table discovery 发现未知 temporal table family 或 row-key prefix | 显式报告 graph、table family、row-key version 和 revision；禁止 iterator 层静默跳过并返回不完整成功。 |
| `TEMPORAL_CLOSED_INTERVAL_CONFLICT` | upsert 命中已关闭 interval，试图原地修改/重开 | 拒绝 mutation；历史订正必须走另立流程，不得改变已关闭事实。 |
| `TEMPORAL_COLOCATION_CAPACITY_EXCEEDED` | 共置组 `(graph, temporal_label, entity_id, fact_key)` 达到单 Region 行数/字节/写入速率上限 | 拒绝写入；返回共置组与阈值诊断；不得静默跨 Region 拆分同一事实序列，不得降级到其他 backend。 |

`mutation_id` 的信任边界固定为客户端生成、全局唯一并由客户端在重试中复用；Server/Store 不另行生成替代 id。Server 必须在提交前按 `mutation_id` 查重，并比较 canonical fact key、schema_version、valid interval 和 payload hash：同值重放返回原提交结果或 no-op，不同值返回 `IDEMPOTENCY_CONFLICT`。客户端生成不代表 Server 信任客户端声明的 fact key，Server 仍必须根据 schema 重新计算 canonical bytes/hash，并将原始 fact key 与 hash 一并存储。

查询契约补充：`as_of`/`between`/`overlap` 的主路径必须 fact-scoped（`entity_id + temporal_label + fact_key`）。实体级跨全部 fact_key 扇出不是默认路径；若提供扇出 API，必须强制 `limit`/`page_token`，超限返回 `TEMPORAL_QUERY_LIMIT_EXCEEDED`。

### 5.4 性能门槛冻结表

| 指标 | 场景/规模 | 冻结门槛 | 测量方法 | 状态 |
|---|---|---:|---|---|
| 普通非时序查询 P95 | 同一数据集、启用前后对比 | P95 回归不超过 5% | 固定请求集，各运行 30 分钟，报告 P50/P95/P99 与置信区间 | 待真实集群验证 |
| `as_of` P95/P99 | 单实体、1000 条历史区间 | P95 ≤ 50 ms，P99 ≤ 100 ms | HStore/PD/Raft，预热后 10,000 次 | 待真实集群验证 |
| `between`/`overlap` P95/P99 | 单实体 1000 条、多实体 100 条/实体 | P95 ≤ 100 ms，P99 ≤ 200 ms | 统计索引命中和扫描行数 | 待真实集群验证 |
| append 吞吐 | 顺序/乱序/热点，1 KB mutation | ≥ 1,000 mutation/s，错误率 0 | 固定 16 并发、10 分钟稳态 | 待真实集群验证 |
| temporal 扫描行数 | 索引命中查询 | 不超过返回行数的 10 倍 | 采集 Store/Region scan 指标 | 待真实集群验证 |
| 当前/历史一致性 | 提交前、提交后、重启后 | 0 个不一致样本 | mutation 与双视图逐条对账 | 待真实集群验证 |
| 资源放大 | 1 亿 interval、90 天保留 | 存储放大 ≤ 2.5 倍，compaction 放大 ≤ 3 倍 | 固定保留周期和 mutation 大小 | 待真实集群验证 |

### 5.5 基线 A/B 复核记录

- 基线 worktree：`/Users/mac/Desktop/apache-code/hugegraph-dev/hg-baseline`，提交 `ea06593ab`，无 Temporal 改动。
- 首轮默认 Java 26.0.1 运行受到工具链影响：Temporal 工作树早先的 752 项 core 回归日志未记录 JDK，不能用 Java 26 结果做归因；Java 26 还触发旧 JaCoCo class major version 70 兼容性问题。
- 控制变量后使用同一 Java 11.0.26、Maven 3.9.16、同一五项测试选择器分别运行：基线 `TaskCoreTest` 为 5/5 通过；Temporal 工作树 `TaskCoreTest` 同样为 5/5 通过。运行时 A/B 结论：5 项失败不是 Temporal 改动引入，归入此前 Java 26 工具链/环境债。
- 增量边界复核：`git diff --name-status ea06593ab` 显示 Temporal 生产代码和测试为新增文件；Temporal 包未发现 `META-INF/services`、`ServiceLoader`、静态初始化块或 schema 全局注册钩子。该静态证据与 Java 11 A/B 相互印证。
- 全量 reactor compile 在基线与 Temporal 工作树均受 Lombok 生成方法缺失等既有环境问题影响，仍登记为环境债，不计入 Temporal 回归。
- 证据日志：`/Users/mac/Desktop/apache-code/hugegraph-dev/_out/temporal_phase0_remediation/logs/baseline/task_core_baseline_jdk11.log`、`/Users/mac/Desktop/apache-code/hugegraph-dev/_out/temporal_phase0_remediation/logs/temporal/task_core_temporal_jdk11.log`、`/Users/mac/Desktop/apache-code/hugegraph-dev/_out/temporal_phase0_remediation/logs/task_core_jdk11_ab_summary.log`、`/Users/mac/Desktop/apache-code/hugegraph-dev/_out/temporal_phase0_remediation/logs/temporal_diff_name_status.log`。

### 5.6 Phase 1 复核状态

- Phase 0 契约整改：已完成。
- Temporal core 四类验收测试：15/15 通过。
- 性能门槛：数值已冻结，真实集群测量仍待执行，不得宣称达标。
- 全量 reactor 编译：基线 A/B 证明为既有环境债，不计入 Temporal 改动回归，但环境债仍需登记跟踪。
- `TaskCoreTest` 五项运行时归因：Java 11 A/B 均 5/5 通过，已裁决为既有 Java 26 工具链/环境债，不阻断 Phase 1。
- 旧 schema 互读：正式移出 Phase 1，转入后续 Serializer Compatibility RFC；Phase 1 不以未定义的兼容行为作为关闭条件。
- **Phase 1 状态：已关闭。** Phase 2 开始前仍必须完成真实 HStore/PD/Raft 集群验证，并遵守已冻结的性能门槛；全量编译环境债和 Serializer Compatibility RFC 作为跟踪项保留。

## 6. 分阶段开发计划

### Phase 0：契约冻结与 PoC

产出：时序语义 RFC、对象模型、错误码、分页 token 规范、事务与视图一致性方案、客户端与范围边界说明、性能门槛表。

冻结内容：

- Valid Time、半开区间、`as_of`、`between`、`overlap` 语义；
- `append`、`upsert`、`close`、`delete` 状态迁移和错误码；
- 重叠区间拒绝、幂等键、乱序写入、重试、重复提交；
- 已关闭区间拒绝一切写入，历史订正另立流程；
- `GraphTransaction` 原子提交、当前/历史视图一致性；
- `TemporalBackendStore` 事务归属；
- 回滚后 temporal 数据显式不可用；
- REST 先行，Java/Go client 只做契约评估，SDK 另立任务；
- hugegraph-computer/OLAP 不感知 temporal，不提供 temporal join、时间快照计算、temporal traversal；
- 瞬时事件及独立 event 语义不在本阶段，另立 RFC；
- 旧数据默认 current-only；
- 直接以 HStore/PD/Raft 完成最小读写闭环，不引入 Memory 基准实现；
- 性能门槛表的目标数值、数据规模、集群规格和测量方法。

时间盒：5 个工作日。

退出标准：RFC、状态机、冲突规则、事务/视图一致性、回滚语义、客户端范围、OLAP 范围外清单、event 范围外声明和性能门槛表全部评审通过。PoC 代码只在 `feature/temporal-graph-support` 分支验证，不作为 Phase 0 主干交付；普通图全量回归门槛从 Phase 1 开始执行。

### Phase 1：Core 模型与 schema

时间盒：5 个工作日。产出：`TemporalSchema`、`TemporalInterval`、`TemporalMutation`、`TemporalQuery`、时间解析器、错误码和幂等键模型。验收：区间边界、时间粒度、冲突、关闭区间不可变性和 temporal core 序列化单元测试通过；旧 schema 互读不属于本阶段，须由后续 Serializer Compatibility RFC 验收。

### Phase 2：Backend 抽象与 HStore/PD/Raft 实现

时间盒：10 个工作日。产出：`TemporalBackendStore`、HStore temporal table/index layout、PD/Store/Raft 事务语义。

设计行键：`graph + temporal_label + entity_id + fact_key_hash + time_bucket + valid_from + tie_breaker`；行内保留 canonical 原始 `fact_key` 并在查询侧精确过滤。current view 与 history view 分离，`open_interval_index` 按 fact key 维护；append/upsert 使用客户端 `mutation_id` 幂等；通过 PD/Store/Raft 传递 temporal mutation 和 query；placement 以 `(graph, temporal_label, entity_id, fact_key)` 共置组为最小单位，Region split 只能按完整共置组切分，热点 shard 扩展不得把同一事实序列跨 Region。普通 HStore graph table 读写不改变。验收：真实 HStore/PD/Raft 集群上 append/query/as_of/between/overlap/close/delete、重启恢复、并发写入通过。

### Phase 3：分布式一致性与故障恢复

时间盒：10 个工作日。验证 temporal mutation 纳入 PD/Store/Raft 事务边界、leader 切换、Raft replay、Store 重启、分片迁移、snapshot/restore、增量备份和故障恢复。普通非时序请求继续走原 HStore 路径。

### Phase 4：索引与查询下推

时间盒：10 个工作日。产出：temporal index、`as_of`/`between`/`overlap` 查询下推、扫描行数指标和无界查询保护。验收：索引命中、边界、重复 `valid_from` 区间/幂等键、分页、排序、限制和 capability 错误测试通过。

### Phase 5：REST API 与权限

时间盒：10 个工作日。产出：独立 `/intervals` 或 `/facts` REST 资源、权限、错误码、分页 token、审计字段和 OpenAPI 契约。Java/Go client 只在 REST 契约稳定后另立 SDK 任务。

### Phase 6：灰度、监控与发布

时间盒：10 个工作日。完成 feature flag、监控、压测、资源放大评估、滚动升级、回滚演练和数据保留策略。未达到性能门槛或普通链路回归门槛不得发布。

## 7. 测试与性能验收

### 7.1 语义与模型

必须覆盖：时间解析和粒度、UTC/epoch/ISO 互转、时区/DST 拒绝或归一化、毫秒边界、半开区间边界、`as_of`、`between`、`overlap`、相同 `valid_from` 的重复区间、重复幂等键、payload 不一致的幂等键冲突、乱序写入、重试、append/upsert/close/delete、已关闭区间拒绝写入、历史订正拒绝、旧数据 current-only。

Phase 1 还必须提供并通过以下测试类：`TemporalSchemaTest`、`TemporalIntervalTest`、`TemporalQueryTest`、`TemporalSerializerTest`。当前 `TemporalModelTest` 仅作为过渡聚合测试，不得替代上述四类验收证据。
### 6.2 事务与共存

必须覆盖：普通 mutation 与 temporal mutation 同事务提交、提交前两视图不可见、提交后两视图一致、事务失败回滚、普通查询不返回历史版本、显式 temporal 查询不污染普通查询、无 capability 时明确失败。

### 6.3 后端与故障

在真实 HStore/PD/Raft 集群验证：append/query、并发写、热点分桶、leader 切换、Raft replay、Store/Region 重启、分片迁移、snapshot/restore、滚动升级和回滚。HBase、Memory、RocksDB 不作为本阶段 temporal 验收依据。

### 6.4 API、权限与兼容性

必须覆盖：REST DTO、`/intervals` 或 `/facts` 路径、分页 token、排序、重复 `valid_from` 区间/幂等键、权限、错误码、旧 API 默认请求响应、普通 Gremlin 查询和普通图全量回归。

### 6.5 性能与容量门槛

性能门槛必须在 Phase 0 退出前按目标数据规模、节点规格和集群拓扑填写实测目标；数值未填写前不得进入 Phase 2：

| 指标 | 数据规模/场景 | 目标门槛 | 测量方法 | 失败处理 |
|---|---|---:|---|---|
| 普通非时序查询 P95 回归 | 启用 temporal 前后同一数据集 | 不得统计显著回归 | 固定请求集并报告置信区间 | 阻断发布 |
| `as_of` 查询 P95/P99 | 单实体、固定历史深度 | Phase 0 冻结数值 | 真实 HStore 集群压测 | 阻断 Phase 2/4 |
| `between`/`overlap` P95/P99 | 单实体与多实体 | Phase 0 冻结数值 | 统计索引命中和扫描行数 | 阻断 Phase 4 |
| append 吞吐 | 顺序、乱序、热点实体 | Phase 0 冻结数值 | 固定并发和 mutation 大小 | 阻断 Phase 2 |
| temporal 查询扫描行数 | 索引命中场景 | 不允许无界增长 | 采集 Store/Region scan 指标 | 阻断发布 |
| 当前/历史视图一致性 | 提交前、提交后、重启后 | 0 个不一致样本 | 对账 mutation 和两个视图 | 阻断 Phase 2/3 |
| 资源放大 | 存储、compaction、网络 | Phase 0 冻结上限 | 固定保留周期和写入量 | 阻断 Phase 6 |

### 6.6 故障与升级

必须验证 HStore Region/分片重启、HugeGraph 重启、写入中断与重试、重复请求幂等、schema/index rebuild 中断恢复、snapshot/restore、新旧版本滚动升级、回滚到不支持 temporal 的版本、temporal 数据保留和删除策略可观测。

## 8. 交付物与验收门槛

### 7.1 交付物

1. HStore temporal backend 存储和查询实现；
2. temporal index 与查询下推；
3. REST/权限/错误码；
4. HStore/PD/Raft 测试套件；
5. 迁移、监控、压测、升级回滚文档。

### 7.2 合并前硬门槛

- 所有新增 temporal 单元测试通过；
- HStore/PD/Raft 是唯一 temporal backend 目标；HBase、Memory、RocksDB 仅标记为不支持，不得以其测试替代 HStore 验收；
- 现有非时序 core/API/TinkerPop 测试全量通过；
- 旧数据无需迁移即可继续执行普通查询；
- 真实 HStore/PD/Raft 集群上的 append/query/as_of/between/overlap/close、幂等、重启恢复和 Raft replay 测试通过；
- 无静默全表扫描和无界历史返回；
- 有明确 feature flag、回滚后 temporal 数据不可用的显式告警和数据保留方案；
- Java/Go client 的范围结论和 REST 契约文档齐全；
- hugegraph-computer/OLAP 范围外声明和瞬时 event 范围外声明已记录；
- 无 OBKV 代码、依赖或 backend；
- 工作树和提交历史可审计，测试日志写入项目 `_out` 目录。

## 9. 推荐实施顺序

1. Phase 0：完成 RFC、区间状态机、冲突规则、事务/视图一致性、回滚语义、客户端/OLAP/event 范围和性能门槛表；
2. Phase 1：实现 schema/time primitives，并通过普通图回归；
3. Phase 2：实现 HStore/PD/Raft temporal backend，完成真实集群集成测试；
4. Phase 3：完成事务复制、leader 切换、Raft replay、重启恢复和 snapshot/restore；
5. Phase 4：实现 temporal index、查询下推和性能门槛验证；
6. Phase 5：实现独立 REST API、权限、分页和错误码；
7. Phase 6：完成灰度、监控、压测、滚动升级和回滚演练。

不建议第一步直接修改普通 `HugeEdge` ID、`BackendStore.mutate()` 或标准 Gremlin `has()` 逻辑；这些是最容易影响非时序链路的切入点。

## 10. 调研来源与裁决原则

- Neo4j Cypher Manual, Temporal values：时间点、持续时间、UTC、时区、epoch、精度和截断；
- Microsoft SQL Server Temporal Tables：system-time、当前表/历史表、`AS OF`、范围查询、更新/删除版本化语义；
- RushDB Temporal Graphs：事件/状态建模、历史重建和按时间点恢复状态；
- Graphiti temporal knowledge graph 资料：`valid_at`、`invalid_at` 与 `created_at` 的双时态实践；
- HugeGraph 当前仓库代码与 `AGENTS.md`：核心、API、BackendStore、测试 profile 和模块边界。

以上资料只用于设计启发；backend、事务、索引、性能和故障语义以 HugeGraph 代码、HStore/PD/Raft 实际能力和真实压测结果裁定。HBase、Memory、RocksDB 不作为本阶段实现或验收依据。
