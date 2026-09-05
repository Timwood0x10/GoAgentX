# 工具执行 DAG 主线开发计划

> **单一事实来源。** 本文档是工具执行 DAG 的唯一开发依据，替代 `Y1_SINGLE_AGENT_TOOL_DAG_DESIGN.md`（方案 C 已作废）。
> **进度**：M0 ✅、M1 ✅（2026-09-05） | M2–M6 待开发。
> **下一步**：M2（`plannerCognition`：LLM 生长图节点，自己不执行工具）。
> **一句话**：把 ReAct 的 `for round` 循环展开成 `MutableDAG` 上的节点生长——节点 = 一次工具执行，`chatStepState` 私有 PCB 消亡；图既是运行时执行计划，又是进化作动面，又是可观测事实，同一个对象，不再有投影 / 影子 / 事后重建。

---

## 1. 架构：两层同构图

唯一新增概念是**分层**，不是新图类型。两层都是 `engine.MutableDAG`，共用全部算子、全部 patch 执行器、同一个编译器。

```
┌─ L1 能力图（持久 / 进化作动面）───────────────────────────┐
│  节点 = ToolClass：一类工具执行                             │
│    ID = toolName + "#" + argShape        （稳定、跨会话）    │
│    Metadata = { enabled, budget, prior } （进化可 patch）    │
│  边 = 统计出的先后倾向（不是硬依赖）                          │
│  载体：engine.MutableDAG（就是今天 UpdateLiveDAG 接住的那个） │
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

**L1 是"这个 agent 会怎么干活"，L2 是"这次它怎么干的"。** 进化只改 L1（不需要追着运行时实例跑）；L2 的执行统计回灌 L1 的 fitness。

### 1.1 复用红利（零改动直接可用）

进化侧全部算子操作的是 `MutableDAG`，与节点里装什么无关：

- `MutableDAG` 算子：`AddNode` / `RemoveNode` / `AddEdge` / `RemoveEdge` / `ReplaceNode` / `SetNodeMetadata`（`internal/workflow/engine/mutable_dag.go`）
- `DAGPatchExecutor`：`Snapshot / Restore / CanApply / Apply`（`workflow/engine/dag_patcher.go`）
- `WorkflowGenome` 九算子：`InsertNode / RemoveNode / ReplaceNode / Parallelize / Serialize / Swap / Split / Merge / SetMetadata`（`evolution/genome/workflow_genome.go:43`）
- `UpdateLiveDAG`（`ares_bootstrap/provide_new_evolution.go:364`）、`GraphEventHub` 与六种 `ChangeType`（`workflow/engine/graph_events.go`）

**换掉节点语义，进化系统一行不改就作用于工具执行过程。** 这是选 `MutableDAG` 而不是新造结构的全部理由。

---

## 2. ReAct 如何被取消

今天的一轮（`chat_cognition.go:309 chatStep`）：`1 次 Chat API → 0..N 个 ToolCall → 逐个 CallTool → 观察 append 回 Messages → Round++`。

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

| ReAct 里的东西 | 图上的对应物 | `chatStepState` 字段 |
|---|---|---|
| `Round` | 图深度（plan 节点链长度） | ❌ 删 |
| `MaxRounds` | 生长深度上界（L1 策略 / 护栏） | ❌ 删（移到护栏） |
| `Messages[]` | 前驱节点 Output 沿路径拼装（见 §2.2） | ❌ 删 |
| `ToolUses` | L2 图上同 ToolClass 的实例节点计数 | ❌ 删 |
| `Prompt` / `Params` | 会话级不变量，L2 图根节点 Metadata | ❌ 删（挪位） |

**`chatStepState` 整体消亡**，正面兑现"取消 ReAct"。附带：`decodeChatStepState` 的 schema 版本校验、两处重复的 `stepSchemaVersion` 常量、yield/resume 时的 PCB 序列化全部消失。每个节点是**一次性单量子**（`StepOutcome{Done:true}`），不再需要跨量子续跑私有状态。

### 2.1 三种执行体（全部实现同一个 `Cognition`）

契约不变：`agentfabric/executor.go:18` `ExecuteStep(ctx, *models.Task) (*StepOutcome, error)`。派发链路也不变：`Task.Capability` → scheduler 打分 → `fabricAgentExecutor.ExecuteStep`（`kernelscheduler/fabric_executor.go`）→ agent 的 Cognition。**节点的 capability 就是路由键，无需新增派发机制。**

| Cognition | capability | 职责 | 调 LLM |
|---|---|---|---|
| `toolCognition` | `tool/<name>` | 从 payload 取 args → `ToolBinder.CallTool` 一次 → 结果进 `StepOutcome.Result`（调度器经 `buildQuantumStep` 重包进 fabric envelope，即 Output 落点）→ `Done:true` | ❌ |
| `plannerCognition` | `ares/plan` | 沿前驱路径拼上下文（按 ID join fabric envelope 读前驱 Output）→ 调 1 次 LLM → 把 tool call **AddNode/AddEdge 到 L2 图** → `Done:true` | ✅ 1 次 |
| `answerCognition` | `ares/answer` | 拼终答 → `Done:true` | ✅ ≤1 次 |

`ToolBinder`（`chat_cognition.go:60`：`CallTool / ListTools / IsToolIdempotent / GetToolSchemas`）是现成端口，`toolCognition` 直接用。

**关键点：`plannerCognition` 自己不执行工具，只生长图。** 执行权归调度器。工具执行由此成为一等调度实体，天然获得 fabric 的重试、优先级、抢占、租约、崩溃恢复、依赖就绪——这些今天 ReAct 循环体内的工具调用一样都没有。

### 2.2 Output 落点：决策 C（节点不存 Output）

**图只存拓扑 + Metadata（是计划，不持有执行事实）；Output 永居 fabric 任务的 checkpoint envelope，读时按 `节点ID = 任务ID` join。**

读前驱 Output 的生产路径：`Task(id).Checkpoint → DecodeCheckpoint → StepCheckpoint`，读侧即 `taskfabric/plan_loop.go:430 collectOutput`。

| 落点选项 | 裁决 |
|---|---|
| A. Cognition 自己写回图节点（需给 `Cognition` 加图句柄） | ❌ 违反不变量 §7-1（不动 `Cognition` 接口） |
| B. 回写器把 envelope 抄回 L2 节点（图 / fabric 双写） | ❌ 是 `toolprojection` 事后投影的翻版，一致性自找麻烦；M3 拼上下文可以 join 查，无人需要"图上可读 Output" |
| **C. 节点不存 Output** | ✅ **定案，代码已按此实现** |

**M3 / M6 必须遵守**：M3 上下文拼装 = 沿图路径取前驱节点 id → 查对应 fabric task envelope 解 Output；M6 回灌同理。代价：内存 fabric 重启后前驱 Output 蒸发——这是 §8"恢复能力 = 图可重建性"的已认账后果，兜底是"图可重建 + 幂等重编译不产生 `ErrTaskExists`"（M1 已固化，见 §9 M1 落点）。

---

## 3. 增量编译器（M0，主线第一块砖）

这是整条主线的**唯一硬阻塞**：M2 起 `plannerCognition` 运行时长节点，编译器必须能**增量**响应单个图变更，否则一定卡死在 `ErrTaskExists`。

原 `CompileDAG`（`planprojection/coordinator.go`）是全量重编译（先 `Delete` 上一批全部 `PlanIDs` 再 `CompilePlan` 全量重建），三处会立刻打死运行时生长：

1. `Fabric.Delete` 只允许 READY/COMPLETED/FAILED，RUNNING/SUSPENDED 返回 `ErrTaskUndeletable`；
2. 残留旧任务 → 相同 ID 重建 → `ErrTaskExists` → 整批 rollback（all-or-nothing）；
3. `CompilePlan` 要求依赖闭包在同批次内，新长出的 tool 节点依赖的 plan 节点已 COMPLETED、不在新批次 → 编译失败。

**改法：按 ChangeType 精确响应。** 全量路径保留（供冷启动 / `ResetFromSteps`），事件驱动只走增量：

| ChangeType | 增量动作 | 依赖能力 |
|---|---|---|
| `ChangeAddNode` | 创建 **1** 个任务 | `CompileNode`：依赖可解析到 fabric 已存在任务 |
| `ChangeRemoveNode` | 删 **1** 个任务（非终态则跳过并记账，不静默） | `Delete` |
| `ChangeAddEdge` / `ChangeRemoveEdge` | 更新 **1** 个任务的 `Dependencies` | `SetDependencies`（READY 态才允许） |
| `ChangeSetNodeMetadata` | 只更新 payload，**不重建任务** | `UpdatePayload`（非 RUNNING 才允许） |
| `ChangeReplaceNode` | 删旧 + 建新，边继承 | 上面两者组合 |

**依赖跨批解析的正确形式**：批次内 → fabric 已存在任务 → 都没有才报错。与 `depsCompletedLocked`（`taskfabric/dag.go`）天然兼容——它只查"依赖任务存在且 COMPLETED"。已 COMPLETED 的 plan 节点让新 tool 节点立即 READY，正是想要的行为。

> M0 已交付，落点见 §9。

---

## 4. 里程碑序列 M0–M6

一个终态，按依赖砌砖；每一步都不产生"以后要推翻的中间设计"。**验收断言 + 该步必须遵守的前置约束**合并在同一行。

| 步 | 交付 | 验收 + 前置约束 |
|---|---|---|
| **M0** ✅ | 增量编译器（§3 全部五种 ChangeType + `SetDependencies` / `UpdatePayload` / 依赖跨批解析） | 一次 `AddNode` 只建 1 任务、其余 `CreatedAt` 不变；依赖已 COMPLETED 任务的新节点编译后 `IsReady()==true`；RUNNING 任务存在时 `AddNode` 编译成功且不影响 RUNNING；`SetNodeMetadata` 后 `CreatedAt` 不变、payload 已变 |
| **M1** ✅ | `toolCognition`（`tool/<name>`）+ `answerCognition`（`ares/answer`）+ L2 图容器 + 会话根节点 + `routerCognition`；**走真 `Scheduler.Run`**，Output 落点 = fabric envelope（决策 C） | 3 节点 L2 图（grep→read→answer）经 `CompilePlan` 编译成任务、单 agent 声明全 capability 集、真调度器 drain 跑出结果、各任务 envelope 按 ID join 读出结果。**R.3 灰度闸门 `DAGExecution`（默认关、零值=老行为）在本步建立**，不是 M4 才建 |
| **M2** | `plannerCognition`：LLM → AddNode/AddEdge，不自己执行工具 | 需要 2 轮工具的任务在图上长出 2 条 plan 链 + 对应 tool 节点，`GetExecutionOrder()` 无环。**R.1 并发约束**：每会话 L2 图**单 writer**（`plannerCognition` 是唯一写者），编译事件消费管线 per-graph 串行；高频 AddNode 下乱序到达的 `AddEdge` 不得让 `CompileNode` 报 "depends on unknown task"（串行化或延迟解析，二选一并写进实现说明） |
| **M3** | 上下文从图路径拼装（取代 `Messages[]`） | 同一任务用图路径拼出的 prompt 与旧 ReAct 的 `Messages` 语义等价（同工具序列、同观察内容）。此即 **R.3 双跑对拍断言**：DAG 路径 vs ReAct 路径同任务对拍 |
| **M4** | 删 `chatStepState` / `chatStep` / `sub` ReAct / `toolprojection` / 两处 `stepSchemaVersion` | `grep -rn "chatStepState\|stepSchemaVersion\|toolprojection" internal/` 0 命中；全量测试通过。**R.3 前置（不可逆删除的闸门）**：M2–M3 双跑对拍通过且外部行为一致才允许动手，任一侧不达标即停在双跑 |
| **M5** | L1 能力图：`buildLiveAgentDAG` 重写为 ToolClass 图；`plannerCognition` 读 `enabled/budget/prior` 约束生长 | 某 ToolClass 置 `enabled=false` → 该类工具节点不再长出；`budget=1` → 最多长出 1 个实例。**R.4 第一步**：`argShape` 按**类型签名**归一（如 `read_file(path:string, offset:int)`），**不按取值**——否则 LLM 参数微变即长出新 ToolClass，L1 爆炸、进化失去稳定靶子 |
| **M6** | L2 → L1 统计回灌 fitness | 两个仅 L1 Metadata 不同的基因，成功率高的一侧被 GA promote（`tool_weight>0`） |

**M0 之前不写任何 Cognition** —— 编译器不增量，M2 一定卡死在 `ErrTaskExists`，这是已核实的必然。

---

## 5. 进化闭环（L1 作动 → L2 执行 → 回灌 L1）

| 环节 | 落点 | 现状 |
|---|---|---|
| 变异 | `WorkflowGenome` 九算子作用于 L1 | ✅ 已有，零改动 |
| 下发 | `generateDiffPatches` → `DAGPatchExecutor.Apply` → L1 | ✅ 已有 |
| 约束 | `plannerCognition` 生长 L2 前读 L1 的 `enabled/budget/prior` | 🔨 M5 新增 |
| 归因 | `tool_call` 证据按 `(strategyID, toolClassID)` | ✅ 已落地（`ares_evolution/fitness_aggregator.go:390 WindowToolStep`），`ToolStepID`（`toolName#argShape`）即 L1 的 `ToolClassID` |
| 回灌 | L2 节点 `Output/Error/Duration` 聚合成 L1 fitness | 🔨 M6 新增（替代 `toolprojection`） |
| 护栏 | 工具集上界 / 禁止零工具 / 生长深度上界 | ⚠️ `ValidateToolSet`（`ares_evolution/guardrails.go:484`）已有但在 v1 包；深度上界待加（放 v2 侧） |

**约束点位置的硬约束**：`enabled/budget` 作用在 **advertise 层**（LLM 看不见该工具的 schema），`prior` 只进提示词，**不在 `CallTool` 处拦截**。`plannerCognition` 决定"要不要长出这个节点"比过滤 schema 更直接，且天然可审计（图上有没有节点是事实，不是日志）。`prior/budget` 不剥夺 LLM 自主：只决定"这一类工具还允许长出几次"，生长顺序 / 参数 / 何时停止仍全由 LLM 决定。

---

## 6. 死亡清单（收敛的实质）

不删掉这些，就还是"各自为战"。删除即彻底，不留"以防万一"的旁路。

| 对象 | 位置 | 处置 | 依据 |
|---|---|---|---|
| `chatStepState` + `chatStep` + `decodeChatStepState` | `agentfabric/chat_cognition.go:78/309/279` | **删** | 循环展开成图后无存在意义（§2） |
| `sub/executor.go` ReAct 工具循环 + 第二份 `stepSchemaVersion` | `agents/sub/executor.go:40` | **删**，收敛到 §2.1 三执行体 | 与 chat_cognition 语义重复 |
| `internal/toolprojection` | 整包 | **删** | 事后投影，被 L2 图取代（图就是事实） |
| `tool_projection_worker.go` | `ares_bootstrap/`（`bootstrap_steps.go:492` 启动） | **删**（连启动点一起摘） | `toolprojection` 唯一引用方 |
| `buildLiveAgentDAG`（节点=agent） | `cmd/ares/serve_live_dag.go:30` | **重写**为 L1 能力图构造 | 节点语义错，是分散的根源 |
| `buildEvolutionDAG` 合成 input→process→output | `ares_bootstrap/bootstrap.go:664` | **删** | 占位图，L1 真图取代 |
| `workflow/graph.ToolNode` | `workflow/graph/node.go:81` | **降级**为 `toolCognition` 内部实现或删 | 不在执行主路径 |
| `agentloop/engine.go` ReAct | 仅 `sdk/` 引用 | **冻结**为 legacy 兼容壳，不接主线 | 无生产 cmd 引用，动它是纯风险 |

`internal/ares_evolution`（v1）与 `internal/evolution`（v2）并存**不在本设计范围**——是进化系统内部重复，与节点语义无关。主线只要求：新代码只接 v2 + `MutableDAG`，不再往 v1 加东西。

---

## 7. 不变量（实施时不得违反）

1. **不动 `Cognition` 接口**（`agentfabric/executor.go:18`）与 `StepOutcome` 三字段。新执行体全部实现它，派发链路（`fabricAgentExecutor`）零改动。
2. **不新增第三种图表示。** L1、L2 都是 `engine.MutableDAG`。任何"要不要为 X 建新结构"的念头，回到本文档改设计。
3. **不在 `CallTool` 处做进化拦截**（§5）。约束点是"节点长不长出来"和"schema 要不要 advertise"。
4. **图是唯一事实来源。** 不允许再出现"从事件日志重建执行结构"的代码路径——`toolprojection` 的死因就是这条。
5. **进化只改 L1。** L2 是运行时产物，不接受 patch（否则会追着一次性实例节点跑）。
6. **默认关闭闸门保持**：`tool_weight` 默认 0、`DAGExecution` 默认关，M0–M5 落地后外部行为与今天一致。
7. **删除即彻底。** §6 清单里的东西不留旁路，否则分散会复发。

---

## 8. 诚实的代价

- **M4 是不可逆的大删除**：三条执行体收敛成一套，`sdk/` 之外的 ReAct 全部消失。删之前 M1–M3 必须真跑通（R.3 对拍），否则回退成本极高。
- **每个工具执行变成一个调度任务**：任务数量比今天高一个数量级（一轮 N 个工具 = N+1 个任务）。换来重试/抢占/恢复/依赖就绪全免费，但 fabric 容量与 `ReadyTasks()` 的 O(n) 扫描（`taskfabric/dag.go`）需实测确认。
- **`taskfabric` 纯内存**（无 PG/sql）：崩溃恢复的前提是 L2 图可重建，图丢了任务即孤儿。"恢复能力 = 图可重建性"——见 §2.2 决策 C 与 §9 M1 的重建幂等测试。
- **LLM 调用次数不变**（一轮仍 1 次），但增加图操作与编译开销；plan 节点串行链延迟不会更好，同一轮 N 个工具的并行度会更好。
- **`ares_evolution` v1/v2 并存**不在本设计内，主线不解决、只保证不加剧。

---

## 9. 已交付里程碑落点（M0 / M1）

M0 / M1 已于 2026-09-05 落地，全绿（`go build ./...` / `go vet` / `gofmt` / `golangci-lint` 0 issues / `go test -race` / `make gate`）。开发续接从下表定位代码：

**M0 — 增量编译器**

| 能力 | 位置 |
|---|---|
| `Fabric.SetDependencies`（只许 READY，否则 `ErrTaskNotMutable`） | `taskfabric/fabric.go:916` |
| `Fabric.UpdatePayload`（只拒 RUNNING，保留 StrategyID / StepCheckpoint / UsedExperienceID） | `taskfabric/fabric.go:954` |
| `Fabric.Dependents`（反向边索引，供 ReplaceNode 迁移后继） | `taskfabric/fabric.go:998` |
| `Fabric.CompileNode`（单步编译，1:1 契约） | `taskfabric/workflow_plan.go:154` |
| 依赖跨批解析 `resolveDependencies`（批次内 → fabric 已存在 → 报错） | `taskfabric/workflow_plan.go:189` |
| `CompileCoordinator.ApplyChange`（按 ChangeType 派发，`Skipped` 记账不吞） | `planprojection/coordinator.go:266` |
| 测试 | `planprojection/incremental_compile_test.go`（9 项）、`taskfabric/workflow_plan_incremental_test.go`（14 项） |

设计要点：`ChangeSetNodeMetadata` 走 `UpdatePayload`，payload 落 checkpoint envelope 的 `Payload` 字段（与 `CompilePlan` 创建时同一位置，执行体读取路径不变）；增量重写不进持久化事件（`EventTaskUpdated` 不在 `isMustPersistEvent`，跨重启拓扑靠重编 DAG 重建，有 `TestIncrementalRewritesAreNotPersisted` 固化）。

**M1 — L2 图容器 + tool/answer 执行体 + router 派发**

| 能力 | 位置 |
|---|---|
| `L2Graph` 容器（每会话一张 `MutableDAG`，只存拓扑+Metadata；`ares/root` 根节点携带 prompt/params；非根节点与 fabric 任务按 ID 一一对应） | `agentfabric/l2graph.go` |
| `toolCognition` / `answerCognition` / `routerCognition`（`NewRouterCognition` / `routerCognition.ExecuteStep` 按 `task.AgentType` 派发到 tool/answer，未知 capability 报错） | `agentfabric/l2graph.go:144-164` |
| 执行链路 | `fabric_executor.go:57 ExecuteStep` → `scheduler.go:934 buildQuantumStep`（Done 结果重包进 checkpoint envelope） |
| 端到端验收 | `kernelscheduler/l2_graph_scheduler_integration_test.go`：`TestL2Graph_SchedulerExecutesThreeNodeChain`（3 节点经 `CompilePlan` 编译、`go Scheduler.Run` 跑到 COMPLETED、解三个 envelope 断言）；`TestL2Graph_RecompilesIdempotentAfterRestart`（重建幂等，不产生 `ErrTaskExists`） |
| 容器 + 派发单测 | `agentfabric/l2graph_test.go` |

设计要点：单 agent 声明多 capability（`["tool/grep","tool/read","ares/answer"]`）即可顺序吃下整条链——候选打分取 agent 完整 `Capabilities`（`fabric_executor.go:100`）与任务 capability 求交，无需 spawn 多 agent；根节点是会话不变量不建 fabric 任务；测试编译走 `CompilePlan` 整批（不用 raw `Create`，白得依赖闭包校验 + 环检测回归）。

---

## 10. 与旧文档的关系

- `Y1_SINGLE_AGENT_TOOL_DAG_DESIGN.md` 方案 C（事件投影 + Metadata 回灌）**已作废**：节点是只读投影，作动面靠元数据绕路回灌 ReAct 循环，是"执行结构没有图"前提下的补丁。但 C1–C7 产出物被本主线继承——C1 事件契约 → L2 节点可观测字段；C3 过程级归因（`WindowToolStep` / `ToolStepID`）→ **原样复用**为 L1 `ToolClassID`；C4 作动面（`SetNodeMetadata` + `PatchSetNodeMetadata` + `wfOpSetMetadata`）→ **原样复用**为 L1 patch 通道；C5 参数合并（`agents/strategy.go:55/115/177`）→ 语义迁移到 `plannerCognition` 生长约束；C6 护栏 → 复用。作废的只有 C2（`toolprojection`）。
- `AGENT_OS_CLOSURE_DEV_PLAN.md` 的 N-2"单 agent 内 DAG"子线以本文档为唯一实施依据；其发布措辞在 M4 删除 `chatStepState` 前维持"进化作用于 peer 级 agent 拓扑"。
