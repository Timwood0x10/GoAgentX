# 工具执行 DAG 主线开发计划

> **唯一执行依据。** 本文档取代 `AGENT_OS_CLOSURE_DEV_PLAN.md`（已删除）与 `Y1_SINGLE_AGENT_TOOL_DAG_DESIGN.md`（已删除，方案 C 作废）。
> **核实基准**：2026-09-06 逐条比对代码行号，非推测。凡本文档写 ✅ 的，都能在下面给出的行号处读到实现体；凡写 ⚠️/⛔ 的，都能给出「为什么它今天还不成立」。
> **进度**：M0 ✅ | M1 ✅（经 M1.5 补齐）| **M1.5 ✅ 全部落地（2026-09-06，D1–D5 落地记录见 §4.1）** | **M2 ← 下一步** | M3–M6 待开发。
> **一句话**：把 ReAct 的 `for round` 循环展开成 `MutableDAG` 上的节点生长——节点 = 一次工具执行，`chatStepState` 私有 PCB 消亡；图既是运行时执行计划，又是进化作动面，又是可观测事实，同一个对象，不再有投影 / 影子 / 事后重建。

***

## 1. 架构：两层同构图

唯一新增概念是**分层**，不是新图类型。两层都是 `engine.MutableDAG`，共用全部算子、全部 patch 执行器、同一个编译器。

```
┌─ L1 能力图（持久 / 进化作动面）───────────────────────────┐
│  节点 = ToolClass：一类工具执行                             │
│    ID = toolName + "#" + argShape        （稳定、跨会话）    │
│    Metadata = { enabled, budget, prior } （进化可 patch）    │
│  边 = 统计出的先后倾向（不是硬依赖）                          │
│  载体：engine.MutableDAG                                    │
└──────────────┬───────────────────────────△───────────────┘
        约束生长 │                            │ 统计回灌
                │                            │ (成功率/耗时/成本 → fitness)
┌───────────────▽────────────────────────────┴──────────────┐
│ ─ L2 执行图（每会话一张 / 运行时生长）─────────────────────  │
│  节点 = ToolInstance：这一次工具执行                        │
│    ID = sess/<sid>/d<depth>/<tool>#<seq>  （一次性）        │
│  边 = 真实数据依赖（前驱 Output 进后继 Input）               │
│  载体：engine.MutableDAG（同一类型！）                       │
│  编译：planprojection → taskfabric 任务 → kernelscheduler   │
└───────────────────────────────────────────────────────────┘
```

**L1 是「这个 agent 会怎么干活」，L2 是「这次它怎么干的」。** 进化只改 L1（不需要追着运行时实例跑）；L2 的执行统计回灌 L1 的 fitness。

### 1.1 复用红利（已核实存在，零改动可用）

进化侧全部算子操作的是 `MutableDAG`，与节点里装什么无关：

| 能力                                                                               | 位置（已核实）                                                             |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `MutableDAG` 算子 `AddNode`/`AddEdge`/`RemoveNode`/`ReplaceNode`/`SetNodeMetadata` | `workflow/engine/mutable_dag.go:78`（AddNode）/`:239`（AddEdge）        |
| `DAGPatchExecutor`（Snapshot/Restore/CanApply/Apply，实现 `patch.Restorable`）        | `workflow/engine/dag_patcher.go:56/82`                              |
| `WorkflowGenome` 九算子                                                             | `evolution/genome/workflow_genome.go:43`（`wfOps` 全集）                |
| `WorkflowGenome.SetDAG`（把基因组重指到 live 图）                                          | `evolution/genome/workflow_genome.go:99`                            |
| `UpdateLiveDAG`（下发 live 图给进化执行器 + 重指基因组）                                         | `ares_bootstrap/provide_new_evolution.go:364`（`SetDAG` 调用在 `:380`）  |
| `GraphEventHub` + 六种 `ChangeType`                                                | `workflow/engine/graph_events.go:12-26`/`:49`                       |
| 增量编译订阅（生产已接）                                                                     | `planprojection/coordinator.go:476` ← `cmd/ares/serve_agents.go:98` |

**换掉节点语义，进化系统一行不改就作用于工具执行过程。** 这是选 `MutableDAG` 而不是新造结构的全部理由。

***

## 2. ReAct 如何被取消

今天的一轮（`agentfabric/chat_cognition.go:309 chatStep`）：`1 次 Chat API → 0..N 个 ToolCall → 逐个 CallTool → 观察 append 回 Messages → Round++`。

展开成图，**一轮 = 1 个 plan 节点 + N 个 tool 节点**：

```
        [plan d0]                    ← LLM 调 1 次，产出 N 个 tool call
        ├──> [tool grep #0]          ← 不由 planner 执行，AddNode 到 L2
        ├──> [tool read #1]
        └──> [plan d1]               ← DependsOn = 上面所有 tool 节点
                ├──> [tool edit #0]
                └──> [plan d2]
                        └──> [answer]  ← LLM 不再产出 tool call → 终答节点
```

| ReAct 里的东西          | 图上的对应物                    | `chatStepState` 字段 |
| ------------------- | ------------------------- | ------------------ |
| `Round`             | 图深度（plan 节点链长度）           | ❌ 删                |
| `MaxRounds`         | 生长深度上界（L1 策略 / 护栏）        | ❌ 删（移到护栏）          |
| `Messages[]`        | 前驱节点 Output 沿路径拼装（见 §2.2） | ❌ 删                |
| `ToolUses`          | L2 图上同 ToolClass 的实例节点计数  | ❌ 删                |
| `Prompt` / `Params` | 会话级不变量，L2 图根节点 Metadata   | ❌ 删（挪位）            |

**`chatStepState`（`chat_cognition.go:78`）整体消亡。** 附带：`decodeChatStepState`（`:279`）的 schema 版本校验、两处重复的 `stepSchemaVersion`（`chat_cognition.go:47` / `sub/executor.go:40`）、yield/resume 时的 PCB 序列化全部消失。每个节点是**一次性单量子**（`StepOutcome{Done:true}`），不再需要跨量子续跑私有状态。

### 2.1 三种执行体（全部实现同一个 `Cognition`）

契约不变：`agentfabric/executor.go:18` `ExecuteStep(ctx, *models.Task) (*StepOutcome, error)`。派发链路也不变：`Task.Capability` → scheduler 打分 → `fabricAgentExecutor.ExecuteStep`（`kernelscheduler/fabric_executor.go:58`）→ agent 的 Cognition。**节点的 capability 就是路由键，无需新增派发机制。**

| Cognition               | capability    | 职责                                                                                                                                                                               | 调 LLM  |
| ----------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| `toolCognition`         | `tool/<name>` | 从 payload 取 args（仅 `arg.` 命名空间，D3）→ `ToolBinder.CallTool` 一次 → 结果进 `StepOutcome.Result`（调度器经 `buildQuantumStep`（`scheduler.go:934`）重包进 fabric envelope，即 Output 落点）→ `Done:true` | ❌      |
| `plannerCognition`      | `ares/plan`   | 沿前驱路径拼上下文（按 ID join fabric envelope 读前驱 Output）→ 调 1 次 LLM → 把 tool call **AddNode 到 L2 图** → `Done:true`                                                                        | ✅ 1 次  |
| `answerCognition`       | `ares/answer` | 拼终答 → `Done:true`                                                                                                                                                                | ✅ ≤1 次 |
| `rootCognition` ✅(M1.5) | `ares/root`   | 会话准入：零工作量完成，session prompt（payload `input`）进 envelope；tool 节点的 root 依赖借此落地为真实任务依赖                                                                                                | ❌      |

`ToolBinder`（`chat_cognition.go:60`：`CallTool / ListTools / IsToolIdempotent / GetToolSchemas`）是现成端口，`toolCognition` 直接用。

**关键点：`plannerCognition`** **自己不执行工具，只生长图。** 执行权归调度器。工具执行由此成为一等调度实体，天然获得 fabric 的重试、优先级、抢占、租约、崩溃恢复、依赖就绪——这些今天 ReAct 循环体内的工具调用一样都没有。

**协作也是工具。** `ask_agent` 已经通过 `agentsyscall.BindTools` 绑进 `ToolBinder`（`agentsyscall/syscall.go:30/411/474`），所以它在新模型里就是一个普通 ToolClass 节点。**跨 agent 协作的进化作动器不需要单独立项——M5 给 L1 加上** **`enabled/budget/prior`** **时它自动闭合。**

### 2.2 Output 落点：决策 C（节点不存 Output）

**图只存拓扑 + Metadata（是计划，不持有执行事实）；Output 永居 fabric 任务的 checkpoint envelope，读时按** **`节点ID = 任务ID`** **join。**

读前驱 Output 的生产路径：`Task(id).Checkpoint → DecodeCheckpoint → StepCheckpoint`，读侧参考 `taskfabric/plan_loop.go:430 collectOutput`。

| 落点选项                                      | 裁决                                   |
| ----------------------------------------- | ------------------------------------ |
| A. Cognition 自己写回图节点（需给 `Cognition` 加图句柄） | ❌ 违反不变量 §8-1                         |
| B. 回写器把 envelope 抄回 L2 节点（图 / fabric 双写）  | ❌ 是 `toolprojection` 事后投影的翻版，一致性自找麻烦 |
| **C. 节点不存 Output**                        | ✅ **定案，M1 代码已按此实现**                  |

**M3 / M6 必须遵守**：M3 上下文拼装 = 沿图路径取前驱节点 id → 查对应 fabric task envelope 解 Output；M6 回灌同理。代价：内存 fabric 重启后前驱 Output 蒸发——这是 §9「恢复能力 = 图可重建性」的已认账后果。

***

## 3. 现状台账（核实到行号）

### 3.1 M0 — 增量编译器 ✅ 真实落地

原 `CompileDAG` 是全量重编译，三处会打死运行时生长：`Fabric.Delete` 只允许 READY/COMPLETED/FAILED（`taskfabric/fabric.go:1028`，其余返回 `ErrTaskUndeletable`）；残留旧任务 → 相同 ID 重建 → `ErrTaskExists` → 整批 rollback；`CompilePlan` 要求依赖闭包在同批次内。改法是按 ChangeType 精确响应：

| 能力                                                                | 位置                                                                                          |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `Fabric.SetDependencies`（只许 READY，否则 `ErrTaskNotMutable`）         | `taskfabric/fabric.go:916`                                                                  |
| `Fabric.UpdatePayload`（只拒 RUNNING，保留 StrategyID / StepCheckpoint） | `taskfabric/fabric.go:954`                                                                  |
| `Fabric.Dependents`（反向边索引，供 ReplaceNode 迁移后继）                     | `taskfabric/fabric.go:998`                                                                  |
| `Fabric.CompileNode`（单步编译，1:1 契约）                                 | `taskfabric/workflow_plan.go:154`                                                           |
| 依赖跨批解析（批次内 → fabric 已存在 → 报错）                                     | `taskfabric/workflow_plan.go:189`                                                           |
| `CompileCoordinator.ApplyChange`（五种 ChangeType 派发，`Skipped` 记账不吞） | `planprojection/coordinator.go:266`                                                         |
| 事件订阅 → 增量投影（**生产已接**）                                             | `planprojection/coordinator.go:476` ← `cmd/ares/serve_agents.go:98`                         |
| 测试                                                                | `planprojection/incremental_compile_test.go`、`taskfabric/workflow_plan_incremental_test.go` |

设计要点：`ChangeSetNodeMetadata` 走 `UpdatePayload`，payload 落 checkpoint envelope 的 `Payload` 字段；增量重写不进持久化事件（`TestIncrementalRewritesAreNotPersisted` 固化）。

### 3.2 M1 — L2 图容器 + tool/answer 执行体 ⚠️ 部分落地

**在的**：`L2Graph` 容器（`agentfabric/l2graph.go:31`，每会话一张 `MutableDAG`，只存拓扑+Metadata，`ares/root` 根节点携带 prompt/params）、`routerCognition`/`toolCognition`/`answerCognition`（`l2graph.go:134/169/197`）、端到端测试跑真 `Scheduler.Run`（`kernelscheduler/l2_graph_scheduler_integration_test.go`）、重建幂等测试。

**不在的（M1 验收未达成，已在 M1.5 全部补齐，记录见 §4.1）**：

| 欠账                                                                                                                 | 证据                                                                    |
| ------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------- |
| **未接生产**。无任何非测试代码构造 `NewL2Graph`/`NewRouterCognition`；生产仍是 `newPeerChatCognition`（`cmd/ares/peer_mode.go:309`）     | `l2graph.go:20-22` 注释自陈「NOT yet wired into the production serve path」 |
| **`DAGExecution`** **闸门不存在**。非测试代码 0 命中                                                                            | 闸门是 M2–M3 双跑对拍的载体，而对拍是 M4 不可逆删除的唯一前置——缺它则 M4 无从验收                     |
| **M0 × M1 接缝零覆盖**。M1 的 E2E 走 `CompilePlan` 整批（`l2_graph_scheduler_integration_test.go:181`），不走事件增量路径；M0 的增量测试不带调度器 | 「生长 → 事件 → ApplyChange → drain」这条 M2 完全依赖的链路从未一起跑过                    |
| 三处代码缺陷                                                                                                             | 见 §4 D1–D3                                                            |

***

## 4. M1.5 — 补齐 M1 欠账 ✅ 全部落地（2026-09-06，记录见 §4.1）

**目标**：让「图生长 → 事件 → 增量编译 → 调度」在事件路径下正确、可诊断、可灰度。M1 的缺陷在批量编译路径下全部不可见。

| #    | 任务                                                                                                               | 验收                                                                    |
| ---- | ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| D1 ✅ | `AddToolNode` 改为单次 `AddNode`（前驱写进 `step.DependsOn`），去掉 AddNode + AddEdge 两步                                      | 一次调用只发 1 个 GraphEvent；编译出的任务 `Dependencies` 非空；「边生长边订阅」测试 `-race` 无竞争 |
| D2 ✅ | `GraphEventHub` 丢弃可检测：事件加单调 seq + 订阅端跳号触发全量对账补偿 + 计数告警；或改 per-graph 无界队列 + 背压                                    | >64 连续变更突发后全部节点最终都有对应任务；丢弃有计数有日志                                      |
| D3 ✅ | 节点参数收进命名空间（Metadata `arg.` 前缀或单个 `args` JSON），`argsFromPayload` 只认该命名空间                                          | 一个「未声明参数即报错」的 fake tool 能跑通 tool 节点                                   |
| D4 ✅ | `agentfabric.DAGExecution{Enabled bool}` 闸门，默认 false = 老行为；cognition 工厂按闸门返回 `chatCognition` / `routerCognition` | 闸门关：`make gate` 与今天无差异；闸门开：3 节点链跑通                                    |
| D5 ✅ | 跨接缝集成测试：L2 生长 → `SubscribeGraphEvents` → 真 `Scheduler.Run` → envelope 按 ID join                                  | 该测试存在且 `-race` 绿                                                      |

### 4.1 M1.5 落地记录（2026-09-06）

验证基线：`make fmt` exit 0（`gofmt -s` 干净），`make check` 全绿（lint：vet + staticcheck + golangci-lint 0 issues；test：137 包 ok，0 FAIL），`make gate` 绿，§12 race 集（taskfabric / planprojection / agentfabric / kernelscheduler / evolution / ares\_evolution / ares\_bootstrap）全绿。

| #    | 落点                                                                                                                                                                                                                                                                                                          | 测试                                                                                                                                                                                                                           |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D1 ✅ | `agentfabric/l2graph.go: AddToolNode` 单次 `AddNode`（`DependsOn` 空→无前驱，非空→单前驱）；删 `AddEdge` 两步（该步曾发布无依赖节点事件 + 原地改已发布 `*Step`，与异步订阅者真实数据竞争）                                                                                                                                                                     | `TestL2Graph_AddToolNodeEmitsSingleEvent`（1 事件 + `DependsOn` 在事件 Step 内 + 无第二事件）；D5 测试 `-race` 即「边生长边订阅」覆盖                                                                                                                   |
| D2 ✅ | 选「seq + 对账 + 计数」分支：`GraphEvent.Seq`（hub 锁内分配 + 发布，原子）、`GraphEventHub.Dropped`（per-sub 计数）、`MutableDAG.DroppedEvents`、`CompileCoordinator.Reconcile`（拓扑序创建 + 已跟踪刷新 + stale 删除 + 终态跳过；`ChangeReconcile` 归因）+ `applyAddNode` 的 `ErrTaskExists` 幂等认领；订阅循环做 seq-gap 告警 + 对账、drop 计数轮询（逐事件 + 250ms ticker，覆盖突发尾部） | `TestGraphEventHub_SeqAndDroppedCount`（64 缓冲外精确计数 + 单调 seq）；`TestM15_ReconcileCreatesTasksForMissedBurst` / `AdoptsPreexistingTasks` / `DeletesStaleTrackedTasks`；`TestL2Graph_BurstGrowthConvergesThroughEvents`（70 节点突发收敛） |
| D3 ✅ | 选 `arg.` 前缀分支：`AddToolNode` 经 `argsMetadata` 写命名空间，`argsFromPayload` 只认 `arg.`（剥前缀），`input` / 调度恢复键不再进 `CallTool`；root params 保持原样（root 的 Metadata 从不进工具参数位）                                                                                                                                                | `TestL2Graph_ArgsNamespacedInMetadata`；`TestL2Cognition_StrictToolReceivesOnlyDeclaredArgs`（strictBinder：未声明 key 即报错，payload 含 `input` + `checkpoint` 照样跑通）；E2E chain 仍绿                                                     |
| D4 ✅ | `agentfabric.DAGExecution{Enabled}` + `Select(chat, router)` 纯函数；`cmd/ares/peer_mode.go` 生产接线（默认关 → 与今天无差异；`TODO(tech-debt)` 标 M2 绑定 config + 全量 capability 广告）                                                                                                                                             | `TestDAGExecution_SelectKeepsLegacyBehaviorOff`（关→chat，开→router）；闸门开 3 节点链 = router 本体 E2E（`TestL2Graph_SchedulerExecutesThreeNodeChain` + D5）                                                                               |
| D5 ✅ | `TestL2Graph_IncrementalEventsDriveSchedulerToCompletion`（生长→事件→真 `Scheduler.Run`→envelope join，生长 / 编译 / 执行三方重叠）+ `TestL2Graph_BurstGrowthConvergesThroughEvents`                                                                                                                                          | `-race` 绿（§12 集内）                                                                                                                                                                                                            |

**M1.5 实施中发现并顺手固定的接缝问题**：L2 root 依赖在事件路径下无法编译（`CompileNode` 拒 dangling 依赖，而剥离 root 边会破坏会话顺序 + `GetExecutionOrder` 确定性）——解法：root 作为 admission 任务一起编译，`rootCognition`（`ares/root`）零工作量完成并把 prompt 送进 envelope（§2.1 表已加行）。副作用：M2 planner 可用同一 ID-join 读 prompt；M3 广告 capability 清单应为 `tool/<name>` / `ares/plan` / `ares/answer` / `ares/root`（§5 M3-② 已同步）。批量路径旧 helper 的 root 剥离保留（历史测试路径不动，只增不改）。

**缺陷证据**（D1–D3 为何成立）：

- D1：`AddToolNode` 先 `AddNode`（`DependsOn` 空，`l2graph.go:112`）再 `AddEdge`（`:115`）；`applyAddNode` 用事件里的 `ch.Step`（`coordinator.go:333`）→ `ProjectStep` 拷 `DependsOn`（`projection.go:52`）→ 任务无依赖即 READY 可被调度，随后 `SetDependencies` 撞 `ErrTaskNotMutable` 只进 `Skipped`，依赖永久丢失。且 `AddEdge` 直接 append 到已随事件发布出去的同一 `*Step` 指针（`mutable_dag.go:281-283`），消费 goroutine 无锁读它 → 真实数据竞争。`AddNode` 本就支持依赖（`mutable_dag.go:112-152`，含环检测与回滚）。

- D2：`graph_events.go:91-101` 缓冲 64、满即 `default:` 丢弃，无计数无日志。丢一个 AddNode = 该节点永不成为任务 = 会话静默挂死（违反不变量 §8-4）。

- D3：`ProjectStep` 平铺 `payload["input"]` + 全部 Metadata（`projection.go:60-65`），`ToModelTask` 原样下传（`scheduler.go:1078`，恢复时另加 `payload["checkpoint"]` `:1091`），`toolCognition` 把 payload 全部 key 当工具参数（`l2graph.go:180`）。严格 schema 的工具（MCP `additionalProperties:false`）会直接拒绝。

***

## 5. M2–M6

### M2 — planner 生长节点

- **目标**：LLM 只产图，不执行工具；执行权归调度器。

- **任务**：① `plannerCognition`（capability `ares/plan`）：读前驱 Output → 调 1 次 LLM → AddNode 到 L2；② 会话图注册表：按 `models.Task.SessionID` 建/查/释放（`CognitionFactory`（`executor.go:53`）只在 spawn 时调一次且不带 task，句柄不能靠闭包传）；③ 每会话单 writer + 编译事件 per-graph 串行；④ 生长深度上界护栏；⑤ fabric 终态任务回收（reaper 或 ready 索引）；⑥ **SessionID 贯通（② 的硬前置）**：`taskfabric.Task` 当前无 SessionID 字段，`kernelscheduler.ToModelTask`（`scheduler.go:1066-1094`）只恢复 UserProfile/Payload/StrategyID/checkpoint、**不恢复 SessionID** —— 须把 SessionID 从提交经 fabric 任务一路带到 executor（Task 字段 + checkpoint envelope），否则注册表按 SessionID 建不出键。

- **验收**：需 2 轮工具的任务长出 2 条 plan 链 + 对应 tool 节点；`GetExecutionOrder()` 无环；全部节点都拿到任务；会话结束图被释放；1000 任务下 drain tick 不退化。

- **前置**：M1.5 全绿。

### M3 — 上下文从图路径拼装

- **目标**：`Messages[]` 被图路径取代。

- **任务**：① 沿前驱路径按节点 ID join fabric envelope 取 Output 拼 prompt；② agent 广告全量 capability（`tool/<name>` / `ares/plan` / `ares/answer` / `ares/root`，随工具注册变化；今天只声明一个，`peer_mode.go:315`）。

- **验收**：同一任务闸门开/关两条路径对拍——同工具序列、同观察内容、外部行为一致。

### M4 — 删 ReAct

- **目标**：三条执行体收敛成一套。

- **任务**：按 §7 死亡清单删除。

- **验收**：`rg "chatStepState|stepSchemaVersion|toolprojection" -g '*.go' internal cmd` 0 命中；全量测试通过。

- **前置**：M3 对拍通过且外部行为一致，否则停在双跑（本步不可逆）。

- **详细步骤与生产保障**：见下一节；本节只是摘要，实施以下一节为准。

### M4 执行计划：彻底删除 ReAct

**判定（2026-09-06 实测）：不能直接砍。** 基线已绿（`go test ./...` 137 包 ok、`gofmt -l` 空、`make gate` 绿），但四项前置未达成。

| # | 前置 | 实测 | 为什么阻塞 |
|---|---|---|---|
| P1 | 闸门可开 | `DAGExecution` 生产零值（`peer_mode.go:308`）；`internal/ares_config` 零配置键 | 不改代码打不开，无法灰度 |
| P2 | L2 路径有生产流量 | `Enabled=true` 只出现在 `l2graph_test.go:29`、`m3_context_test.go:158` | 生产请求数 0，全部证据是单元测试 |
| P3 | `chatCognition` 消费者都在闸门后 | 4 处生产构造，**2 处不在闸门后**：`shadow_execution.go:90`、`introspect/dashboard.go:261` | 删即断影子执行与 introspection 面板 |
| P4 | 只有一条 ReAct 路 | `peer_mode.go:145-148` 把 `sub.Agent` 注册进调度器执行池 | `sub/executor.go` 的循环是生产可达的第二条活路 |

**ReAct 为什么在这里**（决定了删除是收敛而非取舍）：首提交的执行模型是 YAML 声明的**静态** DAG（`docs/engine/zh.md §4.2`），全库无 ReAct。轮次工具循环在 `6fe11772`（2026-06-30）进入 `sub/executor.go`，比 `MutableDAG` 出生（`296d12f4`，2026-06-12）**晚 18 天**——图当时已能运行时长节点，缺的是增量编译器（一次图变更 → 一个任务，不重建批次），所以循环留下，并被复制三份。M0 补上了那块缺口，ReAct 的存在理由随之消失。

#### 保障策略：三层，前两层随 D 阶段失效，第三层永久

| 层 | 机制 | 生效期 |
|---|---|---|
| **S1 配置回滚** | `kernel.dag_execution.enabled` 改配置重启即回 ReAct | A → D 前 |
| **S2 影子对拍** | 复用 `shadow_execution.go`（`errShadowToolDenied` 在接口层拒掉工具调用），L2 路径跑任务副本，只比对工具序列，零生产副作用 | B1 |
| **S3 生长护栏 + 可观测** | 深度上界、强制收敛、会话终止原因 | **永久**——D 之后唯一还在的一层 |

**S3 的缺口已补（A2 落地）**：深度耗尽已有强制收敛——`planner_cognition.go` 命中 `depth >= maxDepth` 时 warn 并 `growAnswerNode(..., "max plan depth reached")`，等价于 ReAct 的 `chat_cognition.go:248-255` 降级纯文本。上限现已可配（`kernel.dag_execution.max_plan_depth`，默认 `agentfabric.DefaultMaxPlanDepth = 10`），与 ReAct 的 `MaxRounds` 可配（`peer_agents.go:113` → `subCfg.MaxToolRounds`）对等，无运维能力回退。

#### A 阶段 — 装回滚手柄（可逆，不改默认行为）

**落地（2026-09-06）：A1 ✅ / A2 ✅。** 配置节 `kernel.dag_execution`（`ares_config/config.go:DAGExecutionConfig`）：`enabled` 默认 false，缺省与今天逐字节一致；`max_plan_depth` 默认 0 = planner 默认（`agentfabric.DefaultMaxPlanDepth = 10`，原未导出 `maxPlanDepth` 已导出为单一真相源）。`peer_mode.go` 经 `resolveDAGExecution` / `resolveMaxPlanDepth`（`cmd/ares/dag_execution.go`）接线，`MaxDepth` 传入 `PlannerDeps`；原 `TODO(tech-debt)` 已摘（做完即删）。负 `max_plan_depth` 被 `Validate` 拒绝；resolver 与 planner 双层兜底（非正 → 默认），护栏永不被误关。

| # | 任务 | 验收 |
|---|---|---|
| A1 | 加配置键 `kernel.dag_execution.enabled`（默认 false），`peer_mode.go:308` 从配置读 | 配置 false → `Select` 返回 chat；true → 返回 router；默认值缺省时与今天逐字节一致 |
| A2 | `MaxDepth` 接配置 `kernel.dag_execution.max_plan_depth`（默认 10） | 配置 3 时第 3 层强制收敛为 answer 节点，不再生长 |
| A3 | 会话可观测：图深度、节点数、终止原因（正常收敛 / 深度耗尽） | 深度耗尽各产生一次带 session id 的 warn + 一次计数 |

测试：`TestValidateKernelDAGExecution`（缺省合法/负值拒绝/正值通过）+ `TestLoad_DAGExecutionSection`（yaml 键端到端；缺节保持关闭）+ `TestResolveDAGExecution` / `TestResolveMaxPlanDepth`（表驱动：缺省关、显式开、自定义透传、负值回默认）。planner 行为本身已有 `TestPlannerCognition_MaxDepthForcesAnswer` 覆盖。

#### B 阶段 — 生产对拍（可逆，唯一能消除 P2 的一步）

| # | 任务 | 验收 / 门 |
|---|---|---|
| B1 | 影子对拍：同一请求两条路都跑，工具调用被拦下，比对工具名序列 | **机制落地 ✅（见下）**；真实请求采样待闸门开启后（P2），不一致样本逐条定性 |

**B1 落地（2026-09-06）：对拍机制 ✅ / 真实流量采样 ⏳。** `agentfabric.CompareDualPath`（`shadow_compare.go`）：同一请求经 legacy chat 体与 planner 体双跑，arm binder 只广告 schema、调用全部拒绝并记录（零副作用），比对工具名序列；分歧进 `MismatchSample`（verdict 内 + `MismatchArchive` 双归档），永不只记日志。这补上了 `TestM4_DualPathBehaviorConsistency` 的缺口——那个测试只跑了 L2 单臂（注释明写"不跑 chat 体"），而本机制双臂真跑。测试：`TestShadowCompare_MatchOnSameScript`（同脚本序列一致、LLM 轮次相等、零样本）+ `TestShadowCompare_MismatchIsArchived`（分歧 verdict +归档各一条）+ `TestShadowCompare_ZeroSideEffects`（生产面零调用）+ `TestShadowCompare_RequiresInput`（缺件 fail-fast）。`cmd/ares/shadow_execution.go` 的策略级 A/B 不动——那是另一轴（策略间比），本机制是执行体轴；生产采样挂钩待 A1 开闸后的真实任务流。
| B2 | 灰度：部分 peer `Enabled=true`，真实工具调用 | 工具调用次数 / 时延 / 失败率与 ReAct 基线对齐；深度耗尽率低于阈值 |

**B2 落地（2026-09-06）：灰度机制 ✅ / 仿真金丝雀 ✅ / 活体金丝雀 ✅（真模型，真 API）。** 开闸前补上了三块缺件，缺一件开闸即故障：

| # | 缺件 | 落点 | 测试 |
|---|---|---|---|
| B2-1 | 生产会话准入（`InitSession` 零生产调用方——开闸后 planner 首量子必 `ErrSessionNotFound`） | `ensureSessionAdmission`（`cmd/ares/session_admission.go`）：有 `session_id` + 闸门开 → 幂等准入（多轮复用，不 duplicate root）→ 编译 root → 提交的任务自然流向 planner；失败 fail-fast（无半建任务、无半准入会话）；订阅用 `context.WithoutCancel`（请求 ctx 会随 handler 消亡） | `TestSubmitPeerTask_AdmitsSessionFirst` / `ResubmitReusesSession` / `SessionlessUnchanged` / `GateOffIgnoresSession` / `AdmissionFailureCreatesNothing` |
| B2-2 | 会话释放（answer 执行后图句柄不 drop = 订阅泄漏） | `NewRouterCognitionWithPlanner` 加 `sessions` 参 → answer 体执行成功后 `ReleaseSession`（miss 只 warn；nil = legacy 不变） | `TestL2Cognition_AnswerReleasesSession` / `AnswerWithoutSessionsKeepsWorking` |
| B2-3 | C4 提前：开闸 peer 的能力集**替换**（不含 primary）——经核查这是隔离机制本身，不是 bug：legacy 主能力任务只匹配关闸 peer，`ares/*` 只匹配开闸 peer；且调度器在 fabric 已接线时**唯一**候选源就是 fabric 活体（`scheduler.go` C1 注释），静态 `sub.Agent` 池够不着 `ares/*` | `peerCapabilities` 纯函数锁定该分区（开闸集**不得**含 primary，含了就是把 legacy 流量吸进金丝雀） | `TestPeerCapabilities_PartitionTraffic` |
| B2-4 | 深度耗尽计数（B2 验收要"率"，只有 warn 日志给不出数） | `plannerCognition.ForcedAnswers()`（atomic，进程级；读口预留，metrics 端点另接） | `MaxDepthForcesAnswer` 内断言 0→1 |

**仿真金丝雀数字**（`TestCanary_FullStackL2Sessions`，5 会话并发 × 真实调度器 × echo 工具，脚本 LLM）：会话完成 5/5，工具调用 9/9 成功（成功率 100%），单会话时延 60–100ms，`ForcedAnswers` 0。另含 0 工具轮次、单轮多工具两种形状。**附带真修**：仿真抓到 planner 根回退缺口——根未完成时 `readNodeOutput` 返回 `("", nil)` 不触发回退，导致首量子发空 user 消息（真实 provider 直接 400）；已改为 `err != nil || 空白 → 回退 payload input`。

**活体金丝雀数字**（`TestCanaryLiveLLM`，`//go:build e2e`，agnes flash 真模型 + echo 工具零副作用，同一 prompt 双臂真跑）：连续两次 `legacy_seq=["grep"] dag_seq=["grep"]`，3s 内完成，`ForcedAnswers` 0，终端答案非空。跑之前抓到两个真问题：① fixture 的 `ParameterSchema` 漏顶层 `Type: "object"`（生产 schema 都有，provider 400 证明）；② planner 历史缺 assistant 配对（首轮修完重跑仍 5× 重复，才定位到 CompareDualPath 的 DAG 臂不执行工具、前驱全是空洞——harness 局限，不是 planner 问题；活体改走全栈后消失）。结论：真模型在配对历史下首轮即收敛，无重复调用。

**金丝雀约束（运维必读）**：开闸 peer 只收 `ares/*` 会话流量；legacy 主能力任务不得提交给金丝雀拓扑（分区保证它们落到关闸 peer，但提交端仍应分流）。`submitPeerTask` 的 L2 会话提交 `capability` 必须用 `ares/plan`（提交的任务即首个 plan 量子；planner 对非图 plan ID 有 root 回退，已测）。生产对数指标：`EventToolCallCompleted.Success` 分组成功率（金丝雀 vs 基线）+ `ForcedAnswers` 计数/会话数。任一超阈 → 关配置重启（S1），停在双跑。

**门**：B2 不通过就停在双跑，不进 C。这是 §5 M4 前置「否则停在双跑」的落地含义。

#### C 阶段 — 为删除清场（可逆）

**落地（2026-09-06）：C1 ✅ / C2 ✅（冻结，非迁移）/ C3 ✅（死注册摘除 + 恢复绑定 L2 化）/ C4 ✅（已提前至 B2-3）。** 执行中纠正了两处原计划：

| # | 任务 | 落地 |
|---|---|---|
| C1 | 影子执行不再依赖"具体执行体能跑一切"的假设 | 原计划"改用 router"**不可行**：router 跑 L2 任务会经 planner 向**活会话图**长节点（生产副作用），且 planner 不消费策略、A/B 无意义。实际做法：`shadowQuantumRunner` 按 `agentfabric.IsL2Capability` 跳过 L2 任务（中性 `(false, nil)` + `Skipped()` 计数 + 日志），legacy 判决行为逐字节不变。L2 覆盖归 B1 对拍 + B2 金丝雀，不管策略影子要。测试：`TestShadowRunner_SkipsL2Tasks`（4 种 L2 cap 全跳过、计数 4、legacy 照跑）+ `TestShadowRunner_NilTaskFailsFast`（nil 由 panic 改显式错误） |
| C2 | `introspect/dashboard.go:261` | **冻结，不迁**：唯一调用方是 `examples/30-introspect-panel-demo`（demo 运行时，非生产服务），其 agent 是任意 capability，router 接不下来。D 阶段删 `chat_cognition.go` 时一并处理该 example（迁 L2 cap 或删例）。 |
| C3 | 摘 `sub.Agent` 静态池注册 + 恢复绑定按能力分发 | 摘了两处**死注册**：`peer_mode.go` 批量池（fabric 接线时调度器跳过静态池，`scheduler.go` C1 注释为证）+ syscall-spawn 回调里的 `sched.RegisterExecutor`（同一原因；`agentsyscall.Executor` 那一半保留，协作工具靠它）。`kernel.executors` 保持非空 map 传参（scheduler 拷贝，无别名风险）。**恢复绑定已 L2 化**（C3 余量关闭）：`RegisterExecutorForTask` 的工厂按任务能力分发——L2 cap 经 `selectRecoveryBody` 走 `peerRouter` 的 `cognitionExecutor` 适配器（`Done/Checkpoint/Result` 逐字段直通），其余走原 `newPeerExecutor`；`peerRouter` 为空或失败时回退 legacy，任务永不因分发本身被 stranded。测试：`TestSelectRecoveryBody`（6 格：关闸/L2/legacy/空 router 全覆盖）+ `TestNewCognitionExecutor_TranslatesOutcome`（直通 + 错误透传 + 空体构造失败）。 |
| C4 | 分区验证（已提前至 B2-3） | `peerCapabilities` + `TestPeerCapabilities_PartitionTraffic` 锁定：开闸集永不含 primary。 |
| — | 收口检查 | `NewChatCognition` 生产构造点：闸门后 1 处（`peer_mode.go`）+ 策略影子 1 处（测量 harness，C1 后只跑 legacy）+ demo 1 处（冻结）+ B1 对拍 harness 1 处（非服务流量）。**D 仍删不动**：见下。 |

**D 阻塞（书面）：D0 已删 ✅；D1–D4 未动手——测绘后确认计划低估了范围，动手删即违规，原因如下。**

1. **B2 无生产 Numbers**：仿真 + 活体基线已有，生产对数是运维动作，还没跑 → 按本计划"门"条款停在双跑。
2. **§8-6 第一句已裁决 ✅（2026-09-06，用户授权代签）**：见"§8-6 的处置"末段。裁决不代替验收，D 的机械前提不变。
3. **D1–D3 的真实 blast radius（2026-09-06 实测，超出原"D 四行表"）**：
   - `sub.Agent` 是 peer 身份类型（`createPeerAgents` 返回 `[]sub.Agent`，`buildPeerRegistry`、`wireEvolutionIPC`、`peerExecutorAdapter`、`agentsyscall` 全链路依赖），其引擎正是要删的 `sub.TaskExecutor` 循环。删循环 = 重写协作栈（peer 生成/syscall/IPC/恢复），不是机械删除。
   - Legacy 主能力流量仍在被服务：删闸门+chat 后 router 拒收 primary cap，legacy 提交任务将饿死（非失败，是无候选者）。M4 语义上 L2-only 世界要求客户端先迁移，未发生。
   - 策略影子（`shadow_execution.go`）删 chat 即编译中断；其长期归宿（M5/M6 fitness 接管）属进化闭环，不属 M4——D 若动它等于越权删进化功能。
   - 结论：D1–D4 的前置不是"一行 grep"，而是"协作栈 L2 化 + 客户端迁移 + 影子退役"三个项目。本计划 D 表的"约 2300 行"估计只覆盖了本体，未覆盖生态。
4. **D 范围增补**：`examples/30`（C2 余量：D 删 `chat_cognition.go` 时一并迁 L2 cap 或删例）。恢复绑定余量已关闭（C3）。

#### D 阶段 — 删除（不可逆，单个提交内完成）

| # | 对象 | 位置 |
|---|---|---|
| D0 | `toolprojection` 投影器 + `tool_projection_worker.go` + 配置键 | **已删 ✅（2026-09-06）**：整包 + worker（含测试）+ `ToolProjectionConfig`（字段/默认值/校验/单测）+ 传导链测试。核实后删除：worker 默认关闭、`WindowToolStep` 零生产调用方（M6 走普通 `Window`）、包外函数调用方只有测试、yaml 宽松解析（旧配置文件照常加载）。`rg "toolprojection" -g '*.go'` 已归零（同名字段 `ToolStepID` 在 `feedback`/`fitness_aggregator` 是独立字段，未动）。 |
| D1 | `chatStepState` + `chatStep` + `decodeChatStepState` | `chat_cognition.go:78` 起 |
| D2 | 两处 `stepSchemaVersion` + 两处 `defaultMaxToolRounds` | `chat_cognition.go:47/43`、`sub/executor.go:40/36` |
| D3 | `chatCognition` 整体 + `sub/executor.go` 工具循环 | 收敛到 §2.1 三执行体 |
| D4 | **`DAGExecution` + `Select` 本身**，连 `TestDAGExecution_SelectKeepsLegacyBehaviorOff`、`TestM3_GateOffKeepsLegacyBehavior` 一起删 | 必须与 D3 同提交：先删 chat 后删闸门的中间态是 `Select(nil, router)` |
| D5 | `agentloop/engine.go` **冻结不删** | 无生产 cmd 引用，动它是纯风险（§7 已裁定） |

**验收**：`rg "chatStepState|stepSchemaVersion|toolprojection" -g '*.go' internal cmd` 0 命中；`go test ./...` 全绿；`make gate` 绿。

**D 之后的回滚**：配置手柄随 D4 消失，回滚降级为 revert D 提交 + 重新构建部署。因此 D 前必须打 tag 并把回滚 runbook 写进发布说明——这是明账交换，不是遗漏。

#### §8-6 的处置

第一句（`DAGExecution` 默认关）随 D 作废：闸门是过渡工具，路径只剩一条时它没有语义，且保留它同时违反 code_rules_v2 §5.1（禁止并存两套执行循环）与本文 §7（不留「以防万一」的旁路）。§8-6 的措辞本身自带有效期——「M1.5–M5 落地后外部行为与今天一致」。第二句（`tool_weight` 默认 0）**不受影响**，属 M6 进化侧（`ares_config/config.go:1015` → `evolution_lifecycle_config.go:94`）。

**裁决（2026-09-06，用户授权代签）：第一句在 D 提交中废止。** 依据：① 副作用清单已列且可接受——回滚手柄降级为 revert+重部署（D 前打 tag + runbook 入发布说明，明账交换）；能力面放宽已由分区测试锁定（`TestPeerCapabilities_PartitionTraffic`），放宽本身正是金丝雀隔离的机制。② 保留的代价更大：永久双轨违反 §5.1 与 §7，且 §8-6 第一句的字面有效期（M1.5–M5）在 D 完成时届满。③ D 的机械前提（B2 数、C 收口）在本裁决之后仍逐项执行，裁决本身不代替任何验收。

**不在 M4 范围**：`buildLiveAgentDAG` / `buildEvolutionDAG` 重写属 M5。

### M5 — L1 能力图

- **目标**：进化有稳定作动面。

- **任务**：① `buildLiveAgentDAG` 重写为 ToolClass 图，`argShape` 按**声明的参数名集合**归一（`read_file` 声明 `path,offset` 即 `read_file#offset,path`），不按取值、不含类型；② 把「编译进 fabric」从 live DAG key 上解绑（今天 `serve_agents.go:93 CompileDAG(ctx, liveDAG)` + `:98` 订阅会把 ToolClass 节点编成垃圾任务、把每次 L1 变异投影成任务创建），peer 级任务供给另行接入；③ `plannerCognition` 生长前读 L1 `enabled/budget/prior`。

- **验收**：`enabled=false` → 该类节点不再长出；`budget=1` → 最多 1 个实例；`ask_agent` 同受约束（协作 ACT 在此闭合）；L1 节点数不随 LLM 参数微变增长。

### M6 — 统计回灌 fitness

- **目标**：L2 执行结果决定 L1 基因优劣。

- **任务**：L2 节点 `Output/Error/Duration` 按 `(strategyID, toolClassID)` 聚合进 L1 fitness（复用 `WindowToolStep`）。

- **验收**：两个仅 L1 Metadata 不同的基因，成功率高的一侧被 GA promote（需 `tool_weight > 0`）。

- **前置**：§6.1 三项。

***

## 6. 进化闭环（L1 作动 → L2 执行 → 回灌 L1）

| 环节 | 落点                                                                                                            | 现状                                                                                                                     |
| -- | ------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| 变异 | `WorkflowGenome` 九算子作用于 L1（`workflow_genome.go:43`）                                                           | ✅ 已有，零改动                                                                                                               |
| 下发 | `generateDiffPatches`（带 `strategyID` fail-fast，`genome_wiring_system.go:1012`）→ `DAGPatchExecutor.Apply` → L1 | ✅ 已有                                                                                                                   |
| 约束 | `plannerCognition` 生长 L2 前读 L1 的 `enabled/budget/prior`                                                       | 🔨 M5 新增                                                                                                               |
| 归因 | `tool_call` 证据按 `(strategyID, toolClassID)`                                                                   | ✅ 已落地（`ares_evolution/fitness_aggregator.go:397 WindowToolStep`），`ToolStepID`（`toolName#argShape`）即 L1 的 `ToolClassID` |
| 回灌 | L2 节点 `Output/Error/Duration` 聚合成 L1 fitness                                                                  | 🔨 M6 新增（替代 `toolprojection`）                                                                                          |
| 护栏 | 工具集上界（`ares_evolution/guardrails.go:484 ValidateToolSet`，已接进 `dream_cycle.findWinner`）；生长深度上界                 | ⚠️ 深度上界待加（M2 随生长逻辑一起加）                                                                                                 |

**约束点位置的硬约束**：`enabled/budget` 作用在 **advertise 层**（LLM 看不见该工具的 schema），`prior` 只进提示词，**不在** **`CallTool`** **处拦截**。`plannerCognition` 决定「要不要长出这个节点」比过滤 schema 更直接，且天然可审计（图上有没有节点是事实，不是日志）。`prior/budget` 不剥夺 LLM 自主：只决定「这一类工具还允许长出几次」，生长顺序 / 参数 / 何时停止仍全由 LLM 决定。

### 6.1 M6 的判决侧前置（三项真实欠账）

**目标**：让「被 promote」与补丁质量真的相关。三项互相独立，可单独排期，不阻塞 M2–M5。

| #      | 任务                                                                                               | 验收                                                                    |
| ------ | ------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------- |
| **E1** | `Evaluate` 内先取时间锚点，再以同一 `Since/Until` 构造 shadow / baseline 两个 strategy 过滤查询                      | 断言两次查询的 `Since/Until` 相同且**非零**                                       |
| **E2** | 在 `deploymentAdapter.Deploy` 晋升成功后接上 `MonitorAndRollback`                                        | 注入回归窗口 → 记录变 `DeploymentRolledBack` 且 executor 还原为旧实例（指针相等）           |
| **E3** | `gate_eval` 注入 logger 输出结构化 warn（区分 registry / runner / 空 suite）；生产置 `StrictMode=true`；跳过计数上可观测面 | 三种缺失各产生一次带原因的 warn + 一次计数；`StrictMode=true` + registry nil → 返回 false |

**欠账证据**：

- E1：判决用 `delta = shadow - baseline`（`evolution/deployment/deployment.go:236`），两侧来自 `agg.Window(ctx, 候选ID)` 与 `agg.Window(ctx, 活跃ID)`（`ares_bootstrap/deployment_wiring.go:120/127`），但 `evidence.Filter` 的 `Since/Until`（`evidence/evidence.go:73-74`）**从不被设置**（`fitness_aggregator.go:346`），窗口按条数、两次独立 `store.Query`。两处注释（`deployment.go:109-112`、`deployment_wiring.go:88-91`）断言了代码没提供的性质。

- E2：`MonitorAndRollback`（`deployment.go:294`）读 `RollbackThreshold`（`:321`），但**零生产调用方**——全部调用点在 `deployment_test.go`；`deploymentAdapter.Deploy`（`deployment_wiring.go:216`）调完 `dp.Deploy` 就返回。回滚支点不缺（`evolution/patch/patch.go:240/290` Snapshot/Restore 已就绪）。

- E3：`StrictMode`（`ares_evolution/gate_eval.go:36`）生产从不置真（`eval_gate_wiring.go:113` 只覆盖 `MinScore`）；三种缺失的区分只被拼进返回字符串（`:131`），文件连 logger 都没 import；未配置即 `return true`（`:135`/`:176`），无任何运维可见信号。

***

## 7. 死亡清单（收敛的实质）

不删掉这些，就还是「各自为战」。删除即彻底，不留「以防万一」的旁路。

| 对象                                                   | 位置                                                                                                        | 处置                                    | 依据                                                                                  |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------- | ------------------------------------- | ----------------------------------------------------------------------------------- |
| `chatStepState` + `chatStep` + `decodeChatStepState` | `agentfabric/chat_cognition.go:78/309/279`                                                                | **删**                                 | 循环展开成图后无存在意义（§2）                                                                    |
| 两处 `stepSchemaVersion`                               | `chat_cognition.go:47`、`sub/executor.go:40`                                                               | **删**                                 | 单量子不需要跨量子 PCB                                                                       |
| `sub/executor.go` ReAct 工具循环                         | `agents/sub/executor.go`                                                                                  | **删**，收敛到 §2.1 三执行体                   | 与 chat\_cognition 语义重复                                                              |
| `internal/toolprojection`                            | 整包（`projection.go` / `projector.go`）                                                                      | **删**                                 | 事后投影，被 L2 图取代（图就是事实）                                                                |
| `tool_projection_worker.go` + 配置键                    | `ares_bootstrap/tool_projection_worker.go`，启动点 `bootstrap_steps.go:492`，配置 `cfg.Evolution.ToolProjection` | **删**（连启动点与配置键一起摘）                    | `toolprojection` 唯一引用方                                                              |
| `buildLiveAgentDAG`（节点=agent）                        | `cmd/ares/serve_live_dag.go:30`                                                                           | **重写**为 L1 能力图构造（M5，含解绑 `CompileDAG`） | 节点语义错，是分散的根源                                                                        |
| `buildEvolutionDAG` 合成 input→process→output          | `ares_bootstrap/bootstrap.go:664`                                                                         | **删**                                 | 占位图，L1 真图取代                                                                         |
| `workflow/graph.ToolNode`                            | `workflow/graph/node.go:77`                                                                               | **降级**为 `toolCognition` 内部实现或删        | 不在执行主路径                                                                             |
| `agentloop/engine.go` ReAct                          | `agentloop/engine.go:250`，仅 `sdk/` 引用                                                                     | **冻结**为 legacy 兼容壳，不接主线               | 无生产 cmd 引用，动它是纯风险。注意其 `Request.ToolWhitelist`（`:210`）生产从不赋值、且无零交集回退——不要把它当已接线的第三执行体 |

`internal/ares_evolution`（v1）与 `internal/evolution`（v2）并存**不在本计划范围**。主线只要求：新代码只接 v2 + `MutableDAG`，不再往 v1 加东西。

***

## 8. 不变量（实施时不得违反）

1. **不动** **`Cognition`** **接口**（`agentfabric/executor.go:18`）与 `StepOutcome` 三字段。新执行体全部实现它，派发链路（`fabricAgentExecutor`）零改动。
2. **不新增第三种图表示。** L1、L2 都是 `engine.MutableDAG`。任何「要不要为 X 建新结构」的念头，回到本文档改设计。
3. **不在** **`CallTool`** **处做进化拦截**（§6）。约束点是「节点长不长出来」和「schema 要不要 advertise」。
4. **图是唯一事实来源，且事实不得静默丢失。** 不允许「从事件日志重建执行结构」的代码路径（`toolprojection` 的死因），也不允许「图变了但任务没变、没人知道」（D2 的死因）。
5. **进化只改 L1。** L2 是运行时产物，不接受 patch。
6. **默认关闭闸门保持**：`DAGExecution` 默认关、`tool_weight` 默认 0，M1.5–M5 落地后外部行为与今天一致。
7. **删除即彻底。** §7 清单里的东西不留旁路。
8. **验收标准必须是「有生产调用方能到达它」**，不能是 grep 到符号存在就算过。E1/E2/E3 三项欠账全部是 grep 形状或注释形状的验收标准放过去的。

***

## 9. 诚实的代价

- **M4 是不可逆的大删除**：约 2300 行生产代码（`chat_cognition.go` 680 + `sub/executor.go` 1152 + `toolprojection` 363 + worker 144）加其测试。删之前 M2–M3 双跑对拍必须真跑通，否则回退成本极高。

- **每个工具执行变成一个调度任务**：任务数比今天高一个数量级（一轮 N 个工具 = N+1 个任务）。换来重试/抢占/恢复/依赖就绪全免费，但代价是实的：`ReadyTasks`/`ResumableTasks` 每个 tick **全表扫描且持写锁**（`taskfabric/dag.go:33`），而 fabric **不自动回收终态任务**（唯一删除点是调用方驱动的 `Delete`，`fabric.go:1028`），所以 n 在一个 server 生命周期内单调增长。M2 落地时需同批给出 reaper 或 ready 索引，仅跑 benchmark 不足以收口。

- **`taskfabric`** **纯内存**：崩溃恢复的前提是 L2 图可重建，图丢了任务即孤儿。「恢复能力 = 图可重建性」——见 §2.2 决策 C 与 M1 的重建幂等测试。

- **LLM 调用次数不变**（一轮仍 1 次），但增加图操作与编译开销；plan 节点串行链延迟不会更好，同一轮 N 个工具的并行度会更好。

- **里程碑与提交要对齐**：M1 的代码落在标题为 `docs:` 的提交里（`ce4cc947`），M4 既然以「M1–M3 可独立验证」为闸门，之后每个里程碑一个可回退的提交。

***

## 10. 发布措辞边界（对外文档的唯一依据）

凡本节点名未闭环项，不得以「闭环」「完整」表述。

| 编号  | 内容                               | 状态                                                                        | 措辞                                                         |
| --- | -------------------------------- | ------------------------------------------------------------------------- | ---------------------------------------------------------- |
| B-1 | 进化判决的候选特异性（候选在隔离上下文真实执行）         | ✅ 已落地，默认关闭（`evolution.shadow_execution.enabled`）                          | 可写「开启后判决具备候选特异性」；**不得**写「全量 A/B 验证」——受 `sample_size` 与流量限制 |
| B-2 | 单 agent 内部工作流 DAG                | ⛔ 未闭环（M4 删 `chatStepState` 前一律未闭环，M0/M1/M1.5 落地不改变此结论）                    | 写「进化作用于 peer 级 agent 拓扑」，**不写**「作用于单 agent 内部工作流」          |
| B-3 | 三通道（单 agent 任务 / 工具 / 协作）真实反馈进判决 | ✅ 度量已落地，独立 evidence source，默认关闭                                           | 可写「协作与工具的真实成败已作为独立证据源进入进化判决（默认关闭）」                         |
| B-4 | 进化作用于工具选择                        | ✅ 已落地（白名单接线 + 归因入 `EvidenceKey`），需 `tool_weight > 0`                      | 可写「进化可作用于工具选择（默认关闭）」                                       |
| B-5 | 进化作用于跨 agent 协作                  | ⛔ 未闭环（作动器 `ask_agent` 已存在，但 L1 约束在 M5 才接）                                 | **不得**写「进化作用于跨 agent 协作」，直到 M5                             |
| B-6 | 晋升后回归自动回滚                        | ⛔ 不可达（§6.1 E2）                                                            | **不得**写「有自动回滚保护」                                           |
| B-7 | G3 评测门                           | ⚠️ 未配置即放行且无告警（§6.1 E3）                                                    | **不得**写「四道门全程有效」                                           |
| B-8 | 测试覆盖率                            | ⚠️ 上次测量 59.2% < 65% GA 目标（2026-09-03，此后未复测）；`postgres/repositories` 需真 PG | 非发布硬阻断，属质量欠账                                               |

配置事实（避免文档与运维脱节）：`evolution.shadow_execution` 与 `evolution.channel_feedback` 在 `internal/ares_config` 中有定义，但在 `configs/ares.yaml` 里是**注释块**，根 `ares.yaml` 完全没有。「默认关闭」由 Go 零值保证；运维要开启需自己加键。

## 11. 两个待修的既有缺陷（与主线无关，但已核实）

1. `ask_agent` 在 `serve` 的 default 分支被广告给 LLM 但调用即失败——`ipc.Send` 只在 `comp.NewEvolution != nil` 分支注入（`cmd/ares/serve_agents.go:278-303`）。
2. `agentsyscall.SetAskAgent`（`syscall.go:179`）无同步写一个每次工具调用都读的字段（`:415/:419`），注释声称「单写多读安全」——按 Go 内存模型不成立。

## 12. 每步统一的验证动作

```
go build ./... && go vet ./... && gofmt -l . && golangci-lint run
go test -race ./internal/taskfabric/... ./internal/planprojection/... \
              ./internal/agentfabric/... ./internal/kernelscheduler/... \
              ./internal/evolution/... ./internal/ares_evolution/... ./internal/ares_bootstrap/...
make gate
git diff --check
```

`make gate` = `scripts/g1_reachability_gate.sh` + `TestG2ConfigContract` + `TestEventContract` + `-race -tags closure` 跑 `ares_evolution` 与 `ares_bootstrap`。
