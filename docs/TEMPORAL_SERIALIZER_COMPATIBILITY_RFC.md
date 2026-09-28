# Temporal Serializer Compatibility RFC

状态：草案（与 `feature/temporal-graph-enhance` 实现同步）
关联：[TEMPORAL_GRAPH_SUPPORT_PLAN.md](TEMPORAL_GRAPH_SUPPORT_PLAN.md) §5.2 / §5.6 / §4 红线 #8

## 1. 背景与范围

设计文档 §5.2 明确：Phase 1 只冻结 temporal core 对象的 UTC/epoch/ISO 毫秒精度与时区拒绝规则，
**旧 schema 反序列化 / 新旧 schema 互读不在 Phase 1 实现**，改由本 RFC 单独定义：

1. payload / row-key / 索引各层的**版本注册表**（谁、在哪、什么值）；
2. **兼容性策略**（当前为 fail-closed 精确匹配，而非跨版本互读）及其理由；
3. **round-trip 不变式**（编码→解码必须逐字段还原）；
4. **旧 / 未知 payload 的显式拒绝规则**（绝不猜测、绝不静默跳过）；
5. **升级路径**（feature flag + fail-closed codec 如何共同保证滚动升级安全）。

本 RFC 不引入新的序列化格式，也不改变普通图旧对象与旧编码（§5.2 红线）。它把**已实现的版本化
基础设施集中登记**，明确当前兼容边界，并把"跨版本互读"如实标注为**延后项**（见 §7）。

## 2. 版本注册表（single source of truth）

temporal 写入 Raft 日志与 Store 的字节流分布在四个独立编码层，每层各带一个版本标识，
**互不复用**（一层升版不隐式升另一层）：

| 层 | 版本常量 | 值 | 位置 | 拒绝点 |
|---|---|---:|---|---|
| Raft wire op | `TemporalWireProtocol.WIRE_OP` = `TemporalMutationHandler.TEMPORAL_MUTATION` | `0x6A` | Raft 日志条目首字节 | 状态机 `onApply`：未注册的 op 显式失败 / 跳过，绝不当普通任务 apply |
| bundle payload | `TemporalMutationBundle.CODEC_VERSION` | `3` | bundle 编码首 4 字节（int） | `TemporalMutationBundleCodec.decode`：`version != CODEC_VERSION` → `IOException` |
| Store row-key marker | `TemporalIntervalCodec.MARKER_VERSION` | `1` | `fact_key` 之后第 1 字节 | `TemporalIntervalCodec.parse`：版本不符 → 返回 `null`（调用方必须跳过） |
| element-index key | `TemporalElementIndexCodec.ELEMENT_INDEX_VERSION` | `1` | element-index key 首字节 | `TemporalElementIndexCodec.parse`：版本不符 → 返回 `null` |
| Server row-key | `TemporalRowKeyCodec.ROW_KEY_VERSION` | `1` | group prefix / fact-key-hash 首字节 | Server 侧 row-key 编码固定前缀 |
| temporal schema | `TemporalWireProtocol.SCHEMA_VERSION` | `1` | capability 元数据 | `TemporalBackendStoreSkeleton.checkCapability`：`schemaVersion` 不符 → `TEMPORAL_UNSUPPORTED_VERSION` |

`TemporalWireProtocol` 是**诊断用注册表镜像**：`CODEC_VERSION` 直接引用
`TemporalMutationBundle.CODEC_VERSION`，`WIRE_OP` 直接引用
`TemporalMutationHandler.TEMPORAL_MUTATION`，`describe()` 输出
`logical / wireOp / codecVersion / schemaVersion` 四元组。镜像与被引用常量**必须恒等**，
由 §6 的一致性测试 pin 住，防止任一侧单独改动导致注册表与实际编码漂移。

### 2.1 bundle payload 字节布局（CODEC_VERSION = 3）

`TemporalMutationBundleCodec` 大端定长 + 长度前缀，字段顺序即契约：

```
int    CODEC_VERSION            // = 3，decode 首读并精确校验
byte   operation                // APPEND=0 / UPSERT=1 / CLOSE=2 / DELETE=3
text   graph / temporalLabel / entityId
bytes  factKey
text   mutationId
int    schemaVersion
long   validFrom / validTo
bool   open
bytes  payload
int    viewCount                // 校验 0..4；APPEND/UPSERT=4，CLOSE/DELETE=0
viewCount * { text name; bytes key; bytes value; long committedRevision }
byte   elementKind              // Phase C 增量：NONE=0 / VERTEX=1 / EDGE=2
text   elementId / elementLabel
// decode 末尾校验 in.available() == 0，否则 "trailing bytes" IOException
```

`text` / `bytes` 均为 `int length + raw`；`readBytes` 对 `length < 0` 或
`length > available` 抛 `IOException`，截断输入抛 `"truncated temporal bundle"`。

## 3. 兼容性策略：fail-closed 精确匹配

**当前策略不是"新旧互读"，而是 fail-closed 精确版本匹配**：解码方只接受与自身
`CODEC_VERSION` / `MARKER_VERSION` / `ELEMENT_INDEX_VERSION` **完全相等**的字节流，
任何更高或更低版本一律显式拒绝（`IOException` 或 `parse` 返回 `null`），
**绝不猜测字段含义、绝不按旧布局误读、绝不静默跳过返回不完整成功**
（对齐 §5.3 `UNKNOWN_TEMPORAL_SCHEMA` 与 §4 红线 #8"可版本化、可回放"）。

选择 fail-closed 而非跨版本互读的理由：

1. **正确性优先**：temporal 字节流承载半开区间与幂等语义，误读一个字段（如把
   `valid_to` 当 `valid_from`）会破坏区间状态机与冲突判定，代价远高于"拒绝并要求升级"。
2. **升级路径已闭环**（§5）：feature flag 保证混合版本集群中 temporal 写**永不上 Raft 线**，
   因此不存在"v3 leader 编码被 v2 follower 拒绝"的在线分歧场景——所有节点开 flag 时
   必然已共享同一 `CODEC_VERSION`。fail-closed 只在**违反升级纪律**（未全量升级就开写）
   或**数据损坏**时触发，此时拒绝正是期望行为。
3. **避免投机复杂度**：跨版本互读需要字段级默认值、迁移矩阵与向后兼容测试，
   在没有真实多版本共存需求前引入属于过度设计（§5.2 将其移出 Phase 1 的本意）。

## 4. round-trip 不变式

对任意合法 bundle `b`：`decode(encode(b))` 必须逐字段还原 `b`，且：

- **字节确定性**：相同输入产生相同字节（无随机 / 无时间戳混入编码），保证 Raft 多副本
  各自编码同一 mutation 得到一致字节、replay 幂等（ledger 命中即 no-op）。
- **无尾随字节**：`decode` 末尾 `in.available() == 0`，多余字节即拒绝。
- **Phase C 增量可回放**：未绑定图元素的 bundle 写 `elementKind=NONE` + 两个空串，
  round-trip 后与 Phase C 之前的 current-only apply 行为逐字节等价（增量不破坏旧路径）。
- **view 数量约束**：`viewCount ∈ [0,4]`，越界即拒绝。

上述不变式由 `TemporalMutationBundleCodecTest`（APPEND/CLOSE/DELETE 三类 round-trip）
与 §6 新增的拒绝 / 一致性测试共同覆盖。

## 5. 升级路径（滚动升级 + 回滚）

fail-closed codec 与 `TemporalFeatureFlag` **共同**构成安全升级路径：

```
1. 全节点部署新版本（flag 默认 off）
   → 期间无任何 temporal 写进入 Raft；普通图读写不受影响
2. 确认所有 Store/Server 节点版本一致（共享同一 CODEC_VERSION / MARKER_VERSION / SCHEMA_VERSION）
3. 打开 hugegraph.temporal.enabled
   → 此后所有 temporal 写都以统一版本编码，follower / replay 必然同版本，无在线分歧
```

**关键不变式（回滚安全）**：feature flag 只拦**新提交**（`HgStoreSessionImpl.temporalMutation`
→ `RES_CODE_EXCESS`；`HgStoreNodeService.addTemporalRaftTask` → `CLUSTER_NOT_READY`），
**不拦 Raft apply / replay**。已提交日志的重放与 leader 切换后的状态机恢复**不受 flag 影响**，
否则关 flag 回滚会导致已提交数据无法重放、状态机卡死。该边界由
`TemporalFeatureFlagTest.replayIsNotGatedByDisabledFlag` pin 住。

**回滚到不支持 temporal 的版本**：普通图继续可用；temporal 数据不得按普通
vertex/edge/property 解读，capability 路径显式返回 `TEMPORAL_UNSUPPORTED_VERSION`
（§5.1 回滚语义冻结裁定 + `TemporalBackendStoreSkeleton.checkCapability`）。

## 6. 测试覆盖（离线可验证）

| 测试 | 覆盖契约 | 位置 |
|---|---|---|
| `TemporalMutationBundleCodecTest` | APPEND/CLOSE/DELETE round-trip 逐字段还原 | hg-store-test |
| `TemporalSerializerCompatibilityTest`（本 RFC 新增） | 未知 / 旧 `CODEC_VERSION` 显式拒绝、截断 / 尾随字节拒绝、版本注册表镜像一致性、row-key marker 与 element-index 对外来版本的 `null` 拒绝 | hg-store-test |
| `TemporalFeatureFlagTest` | flag flip 契约 + replay 不受 flag 拦截（回滚安全边界） | hg-store-test |
| `PartitionStateMachineTemporalReplayTest` | replay 分支未注册 op 显式失败但推进日志、业务拒绝分类 | hg-store-test |
| `TemporalSerializerTest` | `TemporalTime` UTC/epoch/ISO 毫秒精度 + 时区拒绝 | hugegraph-test（Server） |

`TemporalSerializerCompatibilityTest` 新增 pin：

1. **bundle 版本拒绝**：把合法编码首 4 字节篡改为 `CODEC_VERSION+1` / `CODEC_VERSION-1` / `0`，
   `decode` 必须抛 `IOException` 且消息含 `unsupported temporal bundle version`。
2. **结构完整性拒绝**：尾随多余字节 → `trailing bytes`；截断 → `truncated` / 字段长度非法。
3. **注册表一致性**：`TemporalWireProtocol.CODEC_VERSION == TemporalMutationBundle.CODEC_VERSION`、
   `TemporalWireProtocol.WIRE_OP == TemporalMutationHandler.TEMPORAL_MUTATION`、
   `describe()` 含当前 codec/schema 版本，防止镜像漂移。
4. **row-key marker 拒绝**：篡改 `MARKER_VERSION` 字节后 `TemporalIntervalCodec.parse` 返回 `null`
   （而非误读为另一区间）；正确版本可正常 parse（sanity）。
5. **element-index 拒绝**：篡改 `ELEMENT_INDEX_VERSION` 后 `TemporalElementIndexCodec.parse` 返回 `null`。

## 7. 已实现 vs 延后 vs 集群依赖（诚实标注）

**已实现并有离线测试**：
- 四层版本标识 + fail-closed 精确匹配拒绝（bundle / marker / element-index / schema capability）；
- round-trip 不变式、字节确定性、Phase C 增量可回放；
- 版本注册表镜像一致性；
- feature flag 升级门 + replay-ungated 回滚安全边界。

**延后（本 RFC 明确不在当前实现，§5.2 授权另立）**：
- **跨版本互读 / 字段级向后兼容**：当前只拒绝不互读。若未来出现真实多版本共存需求
  （如灰度期必须同时读 v2/v3 bundle），需另立"版本迁移矩阵 RFC"，定义字段默认值、
  upcast/downcast 规则与迁移测试，**不得**通过放宽 fail-closed 来实现。
- **自动数据保留 / 清理**：审计留存 vs 清理策略需独立设计，不投机自建（§6 Phase 6 数据保留项）。

**集群依赖（不可伪造，须真实 HStore/PD/Raft 验证，§6.3 / §7.2）**：
- 真实滚动升级（混合版本节点共存 → 全量升级 → 开 flag）端到端验证；
- 真实回滚演练（关 flag / 降级到不支持 temporal 的版本，普通图可用性 + temporal 显式拒绝）；
- Raft replay / leader 切换 / Store 重启 / snapshot-restore 后 temporal 字节流版本一致性；
- §5.4 七项性能门槛实测。

以上集群项的脚手架与清单见 [TEMPORAL_CLUSTER_VERIFICATION.md](TEMPORAL_CLUSTER_VERIFICATION.md)。
