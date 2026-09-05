# 工具执行 DAG 主线设计（取消 ReAct，MutableDAG 唯一真相）

> 状态：**主线设计，替代 `Y1_SINGLE_AGENT_TOOL_DAG_DESIGN.md` 的方案 C**。
> 进度：**M0 已落地（2026-09-05，见 §4.2）**；M1–M6 未开工。
> 定案要点：节点 = **一次工具执行**；ReAct 消息循环**取消**，展开成图；全系统只有 **一个** 图表示、**一个** 图→任务编译器、**一个** 节点执行契约。
> 日期：2026-09-05

---

## 0. 一句话

**把 ReAct 的 `for round` 循环，展开成 MutableDAG 上的节点生长。** 循环变量（`Round` / `Messages` / `ToolUses`）全部由图结构承载，`chatStepState` 这个私有 PCB 随之消亡。图既是运行时执行计划，又是进化作动面，又是可观测事实——**同一个对象，不再有投影、影子、事后重建**。

---

## 1. 为什么之前是分散的（问题定位，非方案）

已核实的代码事实，四份互不相通的"执行结构"同时存在：

| # | 表示 | 位置 | 节点语义 | 能执行吗 | 能被进化改吗 |
|---|---|---|---|---|---|
| 1 | `chatStepState.Messages[]` | `agentfabric/chat_cognition.go:78` | 无节点，线性消息 | ✅ 生产主路径 | ❌ |
| 2 | live `MutableDAG` | `cmd/ares/serve_live_dag.go:30` | **一个 agent** | ⚠️ 编译成任务但节点不是工具 | ✅ |
| 3 | `toolprojection` 投影图 | `internal/toolprojection` | 一次工具执行 | ❌ 只读，事后重建 | ❌ |
| 4 | `workflow/graph.ToolNode` | `workflow/graph/node.go:81` | 一次工具执行 | ⚠️ 仅 examples/进化装配引用 | ❌ |

**根因**：能执行的（1）没有图；有图的（2）节点粒度错（是 agent 不是工具）；粒度对的（3、4）不在执行主路径上。于是"进化改单 agent 内部行为"只能靠事后投影 + 元数据回灌这种绕路。

主线的目标就是让 (1)(3)(4) 全部死掉，(2) 的节点语义换成工具，成为唯一表示。

---

## 2. 主线架构：两层同构图

唯一新增概念是**分层**，不是新的图类型。两层都是 `engine.MutableDAG`，共用全部算子、全部 patch 执行器、同一个编译器。

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

**L1 是"这个 agent 会怎么干活"，L2 是"这次它怎么干的"。** 进化只改 L1（所以不需要追着运行时实例跑）；L2 的执行统计回灌 L1 的 fitness。

### 2.1 复用红利（不用改一行的部分）

进化侧全部算子操作的是 `MutableDAG`，与节点里装什么无关，因此**零改动直接可用**：

- `MutableDAG` 算子：`AddNode:78` / `RemoveNode:170` / `AddEdge:239` / `RemoveEdge:299` / `ReplaceNode:704` / `SetNodeMetadata:459`（`internal/workflow/engine/mutable_dag.go`）
- `DAGPatchExecutor`：`Snapshot/Restore/CanApply/Apply`（`workflow/engine/dag_patcher.go`）
- `WorkflowGenome` 九个变异算子：`wfOpInsertNode / RemoveNode / ReplaceNode / Parallelize / Serialize / Swap / Split / Merge / SetMetadata`（`evolution/genome/workflow_genome.go:43`）
- `UpdateLiveDAG`（`ares_bootstrap/provide_new_evolution.go:364`）
- `GraphEventHub` 与六种 `ChangeType`（`workflow/engine/graph_events.go`）

**换掉节点语义，进化系统一行不改就作用于工具执行过程了。** 这是主线选择 `MutableDAG` 而不是新造结构的全部理由。

---

## 3. ReAct 如何被取消（核心机制）

今天的 ReAct 一轮（`chat_cognition.go:309 chatStep`）：

```
1 次 Chat API → 0..N 个 ToolCall → 逐个 CallTool → 观察 append 回 Messages → Round++
```

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

| ReAct 里的东西 | 图上的对应物 | `chatStepState` 字段是否还需要 |
|---|---|---|
| `Round` | 图深度（plan 节点链长度） | ❌ 删 |
| `MaxRounds` | 生长深度上界（L1 策略 / 护栏） | ❌ 删（移到护栏） |
| `Messages[]` | 前驱节点的 `Step.Output` 沿路径拼装 | ❌ 删 |
| `ToolUses` | L2 图上同 ToolClass 的实例节点计数 | ❌ 删 |
| `Prompt` / `Params` | 会话级不变量，L2 图根节点 Metadata | ❌ 删（挪位） |

**`chatStepState` 整体消亡** —— 这正面兑现"取消 ReAct"。附带结果：`decodeChatStepState` 的 schema 版本校验、两处重复的 `stepSchemaVersion` 常量、yield/resume 时的 PCB 序列化全部消失。每个节点是**一次性单量子**（`StepOutcome{Done:true}`），不再需要跨量子续跑私有状态。

### 3.1 三种节点执行体（全部实现同一个 `Cognition`）

契约不变：`agentfabric/executor.go:18` `ExecuteStep(ctx, *models.Task) (*StepOutcome, error)`。派发链路也不变：`Task.Capability` → scheduler 打分 → `fabricAgentExecutor.ExecuteStep`（`kernelscheduler/fabric_executor.go`）→ agent 的 Cognition。**节点的 capability 就是路由键，无需新增派发机制。**

| Cognition | capability | 职责 | 是否调 LLM |
|---|---|---|---|
| `toolCognition` | `tool/<name>` | 从 payload 取 args → `ToolBinder.CallTool` 一次 → 结果写 `Output` → `Done:true` | ❌ |
| `plannerCognition` | `ares/plan` | 沿前驱路径拼上下文 → 调 1 次 LLM → 把 tool call **AddNode/AddEdge 到 L2 图** → `Done:true` | ✅ 1 次 |
| `answerCognition` | `ares/answer` | 拼终答 → `Done:true` | ✅ ≤1 次 |

`ToolBinder`（`chat_cognition.go:60`：`CallTool / ListTools / IsToolIdempotent / GetToolSchemas`）是现成端口，`toolCognition` 直接用。

**关键点：`plannerCognition` 自己不执行工具，只生长图。** 执行权归调度器。这就是"节点是工具执行的逻辑"落地的方式：工具执行成为一等调度实体，天然获得 fabric 的重试、优先级、抢占、租约、崩溃恢复、依赖就绪——这些今天 ReAct 循环体内的工具调用**一样都没有**。

---

## 4. 唯一阻塞项：编译器必须先改成增量

这是整条主线的**第一块砖**，也是唯一的硬阻塞。核实结论：

**当前 `CompileDAG`（`planprojection/coordinator.go:65`）是全量重编译**：先 `Delete` 上一批全部 `PlanIDs`（`:86-89`），再 `CompilePlan` 全量重建（`:92`）。配合 `SubscribeGraphEvents:151` 对**任何** `evt.Success`（`:169`）都触发重编译，不看 `ChangeType`。

三处会立刻打死"运行时生长"：

1. **`Fabric.Delete`（`fabric.go:899`）只允许 READY/COMPLETED/FAILED**。RUNNING/SUSPENDED 返回 `ErrTaskUndeletable`，被 `_ =` 忽略。
2. 于是残留旧任务 → `CompilePlan` 用**相同 ID** 重建 → `ErrTaskExists`（`fabric.go:216`）→ **整批 rollback**（`workflow_plan.go:86` all-or-nothing）→ 本次编译全废。
3. **`CompilePlan` 要求依赖闭包在同批次内**（`workflow_plan.go:70-77`）：`step %q depends on unknown step %q`。新长出的 tool 节点依赖的 plan 节点已在 fabric 里（且已 COMPLETED），不在新批次 → 直接编译失败。

顺带：`SetNodeMetadata:459` 也发 `GraphEvent`（`ChangeSetNodeMetadata`）→ 一个纯属性 patch 触发全量重建，语义上完全不必要。

### 4.1 改法：按 ChangeType 精确响应

`CompileDAG` 全量路径保留（供冷启动 / `ResetFromSteps`），新增增量路径，事件驱动只走增量：

| ChangeType | 增量动作 | 依赖的新能力 |
|---|---|---|
| `ChangeAddNode` | 创建 **1** 个任务 | `CompileNode`：依赖允许解析到 fabric 已存在任务 |
| `ChangeRemoveNode` | 删 **1** 个任务（非终态则跳过并记账，不静默） | 复用 `Delete` |
| `ChangeAddEdge` / `ChangeRemoveEdge` | 更新 **1** 个任务的 `Dependencies` | `Fabric.SetDependencies`（新增，READY 态才允许） |
| `ChangeSetNodeMetadata` | 只更新 payload，**不重建任务** | `Fabric.UpdatePayload`（新增，非 RUNNING 才允许） |
| `ChangeReplaceNode` | 删旧 + 建新，边继承 | 上面两者组合 |

依赖闭包放宽的正确形式（`taskfabric/workflow_plan.go`）：依赖解析顺序为 **批次内 → fabric 已存在任务 → 都没有才报错**。这与 `depsCompletedLocked`（`taskfabric/dag.go:77`）天然兼容——它只查"依赖任务存在且 COMPLETED"，从不关心谁编译进来的。已 COMPLETED 的 plan 节点让新 tool 节点立即 READY，正是想要的行为。

**验收断言**：
- 一次 `AddNode` 只创建 1 个任务，其余任务的 `CreatedAt` 不变（证明没有重建）；
- 依赖一个已 COMPLETED 任务的新节点，编译后 `IsReady()==true`；
- 一个 RUNNING 任务存在时执行 `AddNode`，编译成功且 RUNNING 任务不受影响（今天此场景整批失败）；
- `SetNodeMetadata` 后目标任务 `CreatedAt` 不变、payload 已变。

### 4.2 M0 已落地（2026-09-05）

四种 ChangeType 全部接上增量路径，四条断言全部由测试固化为回归项。落点：

| 能力 | 位置 | 说明 |
|---|---|---|
| `Fabric.SetDependencies` | `taskfabric/fabric.go:916` | 只许 READY；其余状态返回 `ErrTaskNotMutable`（新增 sentinel，附带冒犯状态名） |
| `Fabric.UpdatePayload` | `taskfabric/fabric.go:954` | 只拒 RUNNING；走 `DecodeCheckpoint`→替换→`EncodeCheckpoint`，**保留 StrategyID / StepCheckpoint / UsedExperienceID** |
| `Fabric.Dependents` | `taskfabric/fabric.go:998` | 反向边索引，供 `ChangeReplaceNode` 迁移后继 |
| `Fabric.CompileNode` | `taskfabric/workflow_plan.go:154` | 单步编译，1:1 契约 |
| 依赖跨批解析 | `taskfabric/workflow_plan.go:189` | `resolveDependencies`：批次内 → fabric 已存在 → 报错 |
| `CompileCoordinator.ApplyChange` | `planprojection/coordinator.go:266` | 按 ChangeType 派发；`Skipped` 记账而非吞掉 |

**关键设计决定（偏离原文三处，理由如下）**：

1. **`ChangeSetNodeMetadata` 走 `UpdatePayload` 而不是「只更新 payload，不重建任务」的字面实现**——后者没说清 payload 存在哪。落在 checkpoint envelope 的 `Payload` 字段，与 `CompilePlan` 创建任务时写的位置**同一个**，所以执行体读它的路径不变。

2. **`stepFor(dag, id)` 取代直接用 `evt.Change.Step`**：事件处理器是异步的，处理时图可能已经变了。凡是「从图读」的路径（边变更、元数据、ReplaceNode 后继）都先确认节点还在，否则记 Skipped 而不是把「空 step」投影成一个空依赖表——那是在投影一个从未存在过的状态。

3. **增量重写不进持久化事件**：`EventTaskUpdated`（`taskfabric/events.go`）刻意不在 `isMustPersistEvent` 里、也不在 `taskEventType` 映射里。跨重启的拓扑由**重编 DAG** 重建，不靠折叠这些重写。把它写进 store 是改跨重启协议，不是改编译器，得单独评审。此约束有测试固化（`TestIncrementalRewritesAreNotPersisted`）。

**`CompileRecord.StepCount` 的语义随之变成「当前 tracked 任务数」**，不再只是「上次全量编译的 step 数」。增量路径维护 `planIDs` 集合（AddNode 加、RemoveNode 减），所以 `LastCompile()` 在两次全量编译之间依然真实。这正好让既有的 C4/C6 验收测试（断言 AddNode 后 StepCount 3→、RemoveNode 后 2→）无需修改即通过——它们本来就在断言这个语义，只是之前靠「整批重建」凑出来。

**`CompileDAG` 全量路径的错误变得可归因**：回收不掉的任务 id 会折进错误信息（此前是 `_ = Delete` 全忽略，只剩一个 `ErrTaskExists`）。

验收测试：`internal/planprojection/incremental_compile_test.go`（9 项，含 §4.1 四条断言 + 边变更 / RemoveNode 不可删 / ReplaceNode 新 id 与同 id / 失败变更不编译）、`internal/taskfabric/workflow_plan_incremental_test.go`（14 项，原语契约与状态护栏）。`go build ./...` / `go vet` / `gofmt` / `golangci-lint`(0 issues) / `go test -race ./...` / `make gate` 全绿。

---

## 5. 进化闭环（L1 作动 → L2 执行 → 回灌 L1）

| 环节 | 落点 | 现状 |
|---|---|---|
| 变异 | `WorkflowGenome` 九算子作用于 L1 | ✅ 已有，零改动 |
| 下发 | `generateDiffPatches` → `DAGPatchExecutor.Apply` → L1 | ✅ 已有 |
| 约束 | `plannerCognition` 生长 L2 前读 L1 的 `enabled/budget/prior` | 🔨 新增 |
| 归因 | `tool_call` 证据按 `(strategyID, toolClassID)` | ✅ Y.1 C3 已落地（`WindowToolStep`） |
| 回灌 | L2 节点 `Output/Error/Duration` 聚合成 L1 fitness | 🔨 新增（替代 `toolprojection`） |
| 护栏 | 工具集上界 / 禁止零工具 / 生长深度上界 | ⚠️ `ValidateToolSet` 已有但在 **v1 包** `ares_evolution/guardrails.go:484`，深度上界待加 |

**约束点位置的硬约束（沿用 Y.1，仍然正确）**：`enabled/budget` 作用在 **advertise 层**（LLM 看不见该工具的 schema），`prior` 只进提示词。**不在 `CallTool` 处拦截**。区别是现在有了更好的位置——`plannerCognition` 决定"要不要长出这个节点"，比过滤 schema 更直接，且天然可审计（图上有没有这个节点是事实，不是日志）。

`prior` / `budget` 不剥夺 LLM 自主：它决定"这一类工具执行还允许长出来几次"，不给 LLM 画预定步骤。生长顺序、参数、何时停止，仍然全由 LLM 决定。

---

## 6. 死亡清单（收敛的实质）

不删掉这些，就还是"各自为战"。

| 对象 | 位置 | 处置 | 依据 |
|---|---|---|---|
| `chatStepState` + `chatStep` + `decodeChatStepState` | `agentfabric/chat_cognition.go:78/309/279` | **删** | 循环展开成图后无存在意义（§3） |
| `sub/executor.go` ReAct 工具循环 | `agents/sub/executor.go` + 第二份 `stepSchemaVersion:40` | **删**，收敛到 §3.1 三执行体 | 与 chat_cognition 语义重复 |
| `internal/toolprojection` | 整包 | **删** | 事后投影，被 L2 图取代（图就是事实） |
| `tool_projection_worker.go` | `ares_bootstrap/`，由 `bootstrap_steps.go:492` 启动 | **删**（连启动点一起摘） | 同上；它是 `toolprojection` 唯一引用方 |
| `buildLiveAgentDAG`（节点=agent） | `cmd/ares/serve_live_dag.go:30` | **重写**为 L1 能力图构造 | 节点语义错，是分散的根源 |
| `buildEvolutionDAG` 合成 input→process→output | `ares_bootstrap/bootstrap.go:664` | **删** | 占位图，L1 真图取代 |
| `workflow/graph.ToolNode` | `workflow/graph/node.go:81` | **降级**为 `toolCognition` 内部实现或删 | 不在执行主路径 |
| `agentloop/engine.go` ReAct | 仅 `sdk/` 引用 | **冻结**为 legacy 兼容壳，不接主线 | 无生产 cmd 引用，动它是纯风险 |

`internal/ares_evolution`（v1，30 个生产文件）与 `internal/evolution`（v2）并存的问题**不在本设计范围**——它是进化系统内部的重复，与节点语义无关。主线只要求：新代码只接 v2 + `MutableDAG`，不再往 v1 加东西。

---

## 7. 落地顺序（一个终态，按依赖砌砖）

每一步都不产生"以后要推翻的中间设计"。这是"阶段"而非"分期方案"的分界线。

| 步 | 交付 | 依赖 | 验收断言 |
|---|---|---|---|
| **M0** ✅ | 增量编译器（§4.1 全部五种 ChangeType + `SetDependencies` / `UpdatePayload` / 依赖跨批解析） | 无 | §4.1 四条断言全过；RUNNING 任务存在时 AddNode 不再整批失败 —— **已落地 2026-09-05，细节见 §4.2** |
| **M1** | `toolCognition`（`tool/<name>`）+ L2 图容器 + 会话根节点 | M0 | 手写一张 3 节点 L2 图（grep→read→answer），编译后调度器真跑出结果；节点 `Output` 落在图上 |
| **M2** | `plannerCognition`：LLM → AddNode/AddEdge，不自己执行工具 | M1 | 给定一个需要 2 轮工具的任务，图上长出 2 条 plan 链 + 对应 tool 节点；`GetExecutionOrder()` 无环 |
| **M3** | 上下文从图路径拼装（取代 `Messages[]`） | M2 | 同一任务用图路径拼出的 prompt 与旧 ReAct 的 `Messages` 语义等价（同工具序列、同观察内容） |
| **M4** | 删 `chatStepState` / `chatStep` / `sub` ReAct / `toolprojection` / 两处 `stepSchemaVersion` | M3 | `grep -rn "chatStepState\|stepSchemaVersion\|toolprojection" internal/` 只剩 0 命中；全量测试通过 |
| **M5** | L1 能力图：`buildLiveAgentDAG` 重写为 ToolClass 图；`plannerCognition` 读 `enabled/budget/prior` 约束生长 | M4 | 把某 ToolClass 置 `enabled=false`，该类工具节点不再长出；`budget=1` 时最多长出 1 个实例 |
| **M6** | L2 → L1 统计回灌 fitness | M5 | 两个仅 L1 Metadata 不同的基因，成功率高的一侧被 GA promote（`tool_weight>0`） |

**M0 之前不要写任何 Cognition** —— 编译器不增量，M2 一定卡死在 `ErrTaskExists`，这是已核实的必然，不是风险预估。

---

## 8. 不变量（实施时不得违反）

1. **不动 `Cognition` 接口**（`agentfabric/executor.go:18`）与 `StepOutcome` 三字段。新执行体全部实现它，派发链路（`fabricAgentExecutor`）零改动。
2. **不新增第三种图表示。** L1、L2 都是 `engine.MutableDAG`。任何"要不要为 X 建个新结构"的念头，回到本文档改设计。
3. **不在 `CallTool` 处做进化拦截**（§5）。约束点是"节点长不长出来"和"schema 要不要 advertise"。
4. **图是唯一事实来源。** 不允许再出现"从事件日志重建执行结构"的代码路径——`toolprojection` 的死因就是这条。
5. **进化只改 L1。** L2 是运行时产物，不接受 patch（否则会追着一次性实例节点跑）。
6. **默认关闭闸门保持**：`tool_weight` 默认 0，M0–M5 落地后外部行为与今天一致。
7. **删除即彻底。** §6 清单里的东西不留"以防万一"的旁路，否则分散会复发。

---

## 9. 诚实的代价

主线不是零成本，这些必须提前认账：

- **M4 是不可逆的大删除**：三条执行体收敛成一套，`sdk/` 之外的 ReAct 全部消失。删之前 M1–M3 必须真跑通，否则回退成本极高。
- **每个工具执行变成一个调度任务**，任务数量比今天高一个数量级（一轮 N 个工具 = N+1 个任务）。换来的是重试/抢占/恢复/依赖就绪全部免费，但 fabric 的容量与 `ReadyTasks()` 的 O(n) 扫描（`taskfabric/dag.go:33`）需要实测确认。这是 M1 的必测项。
- **LLM 调用次数不变**（一轮仍 1 次），但增加了图操作与编译开销。plan 节点串行链的延迟不会更好，同一轮 N 个工具的并行度会更好。
- **`ares_evolution` v1/v2 并存问题不在本设计内**，主线不解决它，只保证不加剧。

---

## 10. 与旧文档的关系

`Y1_SINGLE_AGENT_TOOL_DAG_DESIGN.md` 的方案 C（事件投影 + Metadata 回灌）**在本设计中作废**：它的节点是只读投影，作动面靠元数据绕路回灌 ReAct 循环，本质是在"执行结构没有图"的前提下打的补丁。

但 C1–C7 的产出物**不是白做的，主线直接继承**：

- C1 事件契约（`ares_events/tool_events.go` 的 `round/seq/success/error/arg_shape`）→ L2 节点的可观测字段
- C3 过程级归因（`feedback/feedback.go:126 ToolStepID` + `ares_evolution/fitness_aggregator.go:390 WindowToolStep`）→ **原样复用**，`ToolStepID`（`toolName#argShape`）即 L1 的 `ToolClassID`
- C4 作动面（`DAGNode.Metadata` + `mutable_dag.go:459 SetNodeMetadata` + `evolution/patch/patch.go:45 PatchSetNodeMetadata` + `wfOpSetMetadata`）→ **原样复用**，是 L1 的 patch 通道
- C5 参数合并（`agents/strategy.go:55 MergeNodeParams` / `:115 ToolBudgetFromParams` / `:177 ApplyPriorHint`）→ 语义迁移到 `plannerCognition` 的生长约束
- C6 护栏（`ares_evolution/guardrails.go:484 ValidateToolSet`）→ 复用，但它在 v1 包内；主线只调用不扩展，加"生长深度上界"时放到 v2 侧

作废的只有 C2（`toolprojection` 投影层）——它是"没有真图"时的替代品，有真图后是纯冗余。

---

## 附录 R — 四个缺口补充（2026-09-05，仅追加，不改原文）

以下四项是正文未覆盖的缺口，按"落地时对应里程碑必须增加的验收/前置条款"表述。本附录不修改正文任何结论。

### R.1 L2 并发改图竞态 → M2 验收增补

`plannerCognition` 的 `AddNode` 与 scheduler drain 可能同时发生，§4.1 只定义了"按 ChangeType 精确响应"，未定义顺序保证。补：

- **每会话 L2 图单 writer**：`plannerCognition` 是唯一写者，编译事件消费管线 per-graph 串行（一张图一个事件消费者）。
- M2 验收增补：**高频 AddNode 下任务依赖不得先于任务存在**——乱序到达的 `AddEdge` 事件不得让 `CompileNode` 报 "depends on unknown task"（串行化或延迟解析，二者取一并写进实现说明）。

### R.2 `taskfabric` 纯内存 → M1 验收增补

`internal/taskfabric` 无任何 PG/sql 存储（2026-09-05 已核实，grep `pgx|postgres|sql.DB` 零命中）。"崩溃恢复白拿"的前提是 L2 图可重建——图丢了任务即孤儿。补：

- M1 验收增补：**kill -9 重启后 L2 图可从 checkpoint/事件重建，RUNNING 节点续跑不丢**；最低验收：重建 + 幂等重编译不产生 `ErrTaskExists`。
- §9 代价清单应视为已隐含本条（内存 fabric 意味着恢复能力 = 图可重建性）。

### R.3 上线闸门不在 M 序列 → M4 前置条件

M4 是不可逆删除，但正文未定义灰度方式。补：

- `DAGExecution` 开关（默认关、零值 = 老行为）**在 M1 建立**，不是 M4 才建。
- M2–M3 期间双跑对拍：同一任务 DAG 路径 vs ReAct 路径，M3 的"语义等价"断言即对拍断言。
- **M4 前置**：双跑对拍通过且外部行为一致，才允许动手删除；任一侧不达标即停止 M4，回退到双跑。

### R.4 L1 的 `argShape` 归一 → M5 第一步

`ID = toolName + "#" + argShape` 若按参数实际取值算，LLM 每轮参数微变就长出新 ToolClass：L1 爆炸、预算与成功率散到模板外、进化失去稳定靶子。补：

- **argShape 按类型签名级归一**（如 `read_file(path:string, offset:int)`），不按取值归一。
- M5 验收增补：**参数取值不同、类型签名相同的两个 L2 实例聚合为同一个 ToolClass**，budget/success-rate 挂在该类上。
