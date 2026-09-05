# Y.1 单 agent 内部"工具执行过程 DAG"设计（方案 C：A×B 融合，已定案）

> 状态：**已定案 = 方案 C（A 与 B 的融合，非 A→B 分期）**。节点语义定为**一次工具执行过程**，不是一个 agent。可执行计划见 **§11**；§2–§5 降级为"融合的两个半边"的来源说明；§8–§10 是经代码核实的事实底座，全部保留。
> 关联：`AGENT_OS_CLOSURE_DEV_PLAN.md` 的 Y.1 / N-2 / N-12（N-12 工具半边已闭）。
> 日期：2026-09-04 · 综述人：Timwood0x10（AI review 协作）

***

## 0. 一句话结论（定案）

把"单 agent 内部如何干活"变成进化可达，**唯一正确的节点粒度是"一次工具执行过程"（ToolStep = 工具 + 入参形态 + 其观察结果），既不是一个 agent（A 的粒度错），也不是一条只能看不能改的轨迹（B 的作动面缺失）**。

因此不做 A→B 的分期，而做**融合**：

| 半边                  | 在 C 中承担的角色                                                                            |
| ------------------- | ------------------------------------------------------------------------------------- |
| **B**（真实轨迹投影）       | 决定**节点从哪来**：从事件日志把真实发生过的工具执行过程投影成节点与边（零 checkpoint 改动，§8.7）                           |
| **A**（节点属性可 patch）  | 决定**节点怎么被改**：节点 `Metadata` 成为进化可 patch 的作动面（需补 metadata 算子/快照/genome 保留，§8.1）         |
| **融合产生的新东西**（两者都没有） | **过程级归因**：`tool_call` 证据按 `(strategyID, toolStepID)` 归因，取代 §9.2 的 `(strategy, agent)` |

运行时 ReAct 的自主性**完全不动**：进化改的是"这个执行过程要不要留、给多少预算/先验"，不是给 LLM 画预定步骤（§6 的排除项依然排除）。

***

***

## 1. 背景与现状（已核实代码事实）

### 1.1 生产执行体是 ReAct 消息循环，不是 DAG

- `internal/agentfabric/chat_cognition.go`：peer 模式生产主路径。

  - 状态 `chatStepState{Round, MaxRounds, Messages[], Prompt, Params}`（`chat_cognition.go:77-85`）——线性消息数组 + 轮次计数器，**无节点/边**。

  - `chatStep`（`:303-382`）每轮：Chat API 一次 → 得到 0..N 个 `ToolCalls` → 逐个执行 → 观察 append 回 `Messages`。

  - `ExecuteStep`（`:184-266`）是**调度量子**语义：一轮不结束就 yield（`StepOutcome{Checkpoint: st}`），调度器在 fabric 的 check point slot 存/取 `chatStepState` 续跑。

- `internal/agents/sub/executor.go`：同样 ReAct 工具循环（`:860-874`），经 `ExecuteStep` 委托（`agent.go:348`）。两条执行体差异仅在 A1.4 端口方式，语义一致。

- `internal/agentloop/engine.go`：同为 ReAct。非 peer 生产主路径。

> **关键**：`chatStepState` 是 **resume 的 PCB（schema 版本校验，§6 持久化规范）**。任何要改动它形状的重构，都会触碰 quantum/checkpoint/yield/调度量子契约。

### 1.2 进化 DAG 基建已完整可复用

- `MutableDAG`（`workflow/engine/mutable_dag.go`）：增/删/替换节点、增删边、`SchedulerType` 字段、线程安全、版本号、GraphEventHub。

- `DAGPatchExecutor`（`workflow/engine/dag_patcher.go`）：`PatchInsertNode/RemoveNode/ReplaceNode/AddEdge/RemoveEdge/SetSchedulerType`，全部直接改 `MutableDAG`。

- `generateDiffPatches`（`ares_evolution/genome_wiring_system.go:962-1058`）：parent snapshot → mutate → child snapshot → differ → `RuntimePatch`。

- `UpdateLiveDAG`（`ares_bootstrap/provide_new_evolution.go:350-432`）：接住 live DAG，`WorkflowGenome.SetDAG` + 重建 graph executor / recovery executor 指向 live DAG。

- `Step` 节点（`workflow/engine/types.go:52-68`）：`ID/AgentType/Input/DependsOn/Timeout/RetryPolicy/RecoveryPolicy/Metadata`。

- 工具白名单旋钮已落地（Y.3-ACT）：`Params["tools"]`（`agents/strategy.go` 的 `ToolWhitelistFromParams`）已接入 `chat_cognition.go:311` 与 `sub/executor.go` 两条执行体，空值=全量。

### 1.3 当前缺口（Y.1 要闭的）

`UpdateLiveDAG` 喂给进化的是 **peer 拓扑（一 peer 一节点）**（`serve_live_dag.go:14-75`），不是单 agent 内部步骤。进化能改 peer 图、能改活跃策略的工具白名单，但**改不到"单个 agent 干活时每一步工具调用的选择/顺序"**——N-2 的"单 agent 内 DAG"部分未闭环。

***

## 2. 方案 A：DAG 节点装载认知（节点 = 一个认知 agent）

### 2.1 是什么

用 DAG 的**单节点**装载一个 agent 的完整 ReAct 执行。节点的**属性**（`Metadata`，或直接并入活跃策略的 `Params["tools"]`/prompt）决定该 agent 内部工具使用行为。进化改节点属性 = 改单 agent 行为。

```
              ┌─────────────────────────────────────────────┐
   peer DAG    │  Step{AgentType: "coder", Metadata: {        │
   node ─────▶ │     tools: "web_search,calculator", ...      │
              │  }   └─ 装载 chatCognition（ReAct 自主循环） ──┤
              └─────────────────────────────────────────────┘
```

### 2.2 影响面（最小）

| 维度         | 影响                                                      |
| ---------- | ------------------------------------------------------- |
| checkpoint | **零影响**——节点只是容器，`chatStepState` 不碰，量子/yield 不变          |
| yield/调度   | 一个节点=一次任务=一次量子，与现有调度完全正交                                |
| 执行体        | **零改动**——ReAct 循环原样跑                                    |
| 进化作动       | 改节点 `Metadata`/策略 `Params["tools"]` → 下一轮该 agent 工具白名单变 |
| 归因         | 直接复用已闭环的会话 `collaboration`/`tool_call` EvidenceKey 归因   |

### 2.3 交付内容

1. `buildLiveAgentDAG` 为每个 peer 节点写入其策略工具白名单（从活跃策略 `Params["tools"]` 读）到 `Step.Metadata`。
2. 进化 mutation 增加对"节点工具白名单"维度的变异（可在 `Params["tools"]` 上，**已存在** `Mutator.mutateTool`）——故**基本无需新算子**。
3. 执行体读取策略白名单过滤——**已落地**（Y.3-ACT）。
4. 装配：把节点属性回流为活跃策略，或直接让执行体从节点 Metadata 读白名单。

> 实质：**Y.1 的 60% 已经通过 Y.3-ACT + 现有 MutableDAG 达成**。A 主要是"把已有的东西接起来 + 把作动语义写清楚"。

### 2.4 优点

- 范围小、风险低、不触碰 checkpoint/yield。

- 完全顺着 ReAct/LLM 自主本质，不削弱灵活性。

- 复用已有全部基建（Y.3-ACT 白名单、MutableDAG、generateDiffPatches）。

- 作动语义清晰："进化决定 agent 该用哪些工具"。

### 2.5 缺点

- "图"的粒度只到 **peer 级/单节点**，尚未表达**单 agent 内部的多步工具轨迹**——不能满足"每一步工具调用都是 DAG 节点"的诉求。

- 进化改的是"白名单集合"，不是"调用顺序/依赖图"。

***

## 3. 方案 B：ReAct 工具调用轨迹投影成 DAG（运行时自主 + 旁路轨迹图）

### 3.1 是什么

保持运行时 ReAct **完全自主**（LLM 每轮决定调哪些工具，不改执行模型）。同时，把**实际发生过的工具调用轨迹**，投影成一张 tool-call DAG：

- 每个出现的工具调用 = 一个 DAG 节点（`nodeID = toolName#seq`）。

- 一轮内的并行工具调用 = 同一层的多个节点（可作为兄弟节点）；

- 跨轮 `依赖前面的观察` = 边（前一轮工具节点 → 后一轮依赖它的节点，按消息里的观察顺序/血缘推断）。

- 整张图**旁路沉淀**（如写 `strategy_shadow` 或新 source / in-memory 投影），**不替代 ReAct 执行模型**。

```
ReAct 实际轨迹                        tool-call DAG（旁路投影）
─────────────────                     ─────────────────────────
Round1: web_search(▸)                [web_search#1] ──────▶ [calc#2]
Round2: code_exec(读取其结果)              ▲                      │
        + file_read                    search#1            code_exec#3
                                             └── file_read#4 (并行)
```

### 3.2 影响面

| 维度         | 影响                                                                                    |
| ---------- | ------------------------------------------------------------------------------------- |
| checkpoint | **旁路投影**，不碰 `chatStepState` 形状（但仍要在 checkpoint 里记录"已展开到哪"，否则 resume 后轨迹续不齐——见 3.3 风险） |
| yield/调度   | 投影发生在 `chatStep` 内部、执行工具后、yield 前，不改变量子边界                                             |
| 执行体        | **小改动**：在 `chatStep` 执行工具调用的循环里加"往轨迹	DAG 记一笔"的 hook（类似 `ToolCallObserver` 的位置）        |
| 进化作动       | patch 轨迹 DAG（增删/替换某类工具节点、调白名单）→ 把作动反向**回灌为下一轮的工具先验/偏好**（不强制）                          |
| 归因         | 复用 tool\_call 证据；轨迹 DAG 节点失败率可作为新的 fitness 维度（可选）                                     |

### 3.3 关键难点（如实列出）

1. **resume 续图**：多量子任务上，`chatStepState` 是 resume 的 PCB。轨迹 DAG 若不在 checkpoint 里带"已展开索引"，resume 后新 quantum 无法续接轨迹。需要在 `chatStepState`（或旁路附加字段）加一个**兼容字段**（如 `ToolTraceSeq`），schema 从 1 → 2。此改动触碰 checkpoint 读的旧版路径（`decodeChatStepState` 需容忍缺字段）。
2. **边/血缘推断**：ReAct 消息里工具观察是 `tool` 角色的 message，通过 `ToolCallID` 关联；但"后一轮工具依赖前一轮哪个观察"是**启发式推断**，没有显式依赖声明。图结构因此是"近似轨迹"，非精确语义——mutation 在这些近似边上作动的含义需写清楚。
3. **作动回灌的语义**：patch 出"删掉某工具节点"后，运行时**不能直接把工具从 LLM 禁用**（那又变回白名单，削弱自主）；更合理是作为**先验/提示**注入（如提示词里标注"此任务准备用 X 工具的倾向降低"）或调整 `Params["tools"]` 白名单的上界。这个回灌通道是 B 的真正新增工作量。
4. **轨迹图的有效性**：若每轮轨迹差异巨大（LLM 高度非确定），沉淀出的图会碎片化，作动噪音大。需按 pattern（相同工具序列的聚合统计）收敛，而非逐条轨迹。

### 3.4 优点

- 精确满足"每一步工具调用 = DAG 节点"，且**不重写执行模型、不削弱 ReAct 自主**。

- 复用 `MutableDAG`/`DAGPatchExecutor`/`generateDiffPatches` 作动轨迹图。

- 图具有真实语义（真实走过的调用），比预定笼子可解释。

- 可渐进：轨迹图先只作"观测/审计"，成熟后再开"回灌作动"。

### 3.5 缺点

- 比 A 复杂一个档次：需处理 resume 续图、边/血缘推断、作动回灌三块新逻辑。

- checkpoint schema 需升版（1→2），触碰持久化规范 §6.1。

- 作动回灌的"先验"落地方式尚无现成通道，需新增（提示词注入或新的先验字段）。

***

## 4. 融合后的分工（取代原 A/B 对比）

原表是"选谁"的对比；定案后它的正确读法是"**各自贡献哪一半**"。

| 维度              | A 贡献                     | B 贡献               | **C（融合）的结果**                          |
| --------------- | ------------------------ | ------------------ | ------------------------------------- |
| 节点语义            | 一个 agent（❌ 粒度太粗）         | 一次真实工具调用           | **一次工具执行过程 ToolStep**（= B 的粒度 + 可改属性） |
| 节点来源            | 手工写进 `buildLiveAgentDAG` | 事件日志投影             | **事件日志投影**（B）                         |
| 作动面             | 节点 `Metadata` 可 patch    | 无（只有观测）            | **节点 Metadata patch**（A）              |
| 执行模型 / ReAct 自主 | 不动                       | 不动                 | **不动**                                |
| checkpoint      | 零影响                      | 零影响（§8.7 已核实走事件日志） | **零影响**                               |
| 归因 key          | `(strategy, agent)`      | 需"哪条轨迹好"           | **`(strategy, toolStepID)`**（融合才成立）   |
| 主要新增工作量         | metadata 算子/快照/genome    | 投影层 + 事件契约统一       | 两者之和，但**只做一次**（分期要做两次）                |

***

## 5. 为什么必须融合，而不是 A→B 分期

初稿曾建议"A 先收口、B 下轮立项"。**该建议作废**，理由是三条都经代码核实：

1. **A 单独跑不成立：粒度错。** A 的节点 = 一个 agent，而 `GetActive` 是全局单例（§8.4），"该 agent 的工具白名单"这个语义在代码里根本不存在。A 若不引入比 agent 更细的节点，作动面永远停在"全局换一套工具集"。

2. **B 单独跑不成立：只有观测面，没有作动面。** B 投影出的图，进化要改它必须走 `MutableDAG`/`DAGPatchExecutor`，而节点属性维度的 patch/diff/快照**根本不存在**（§8.1）。B 没有 A 的 metadata 算子，就只是一张审计图。

3. **两者的公共缺口只有融合后才补得上：过程级归因。** §9.2 指出证据全部记到同一个 `strategyID`。A 的对策是加 `agent_id`，B 需要"哪条轨迹好"。**若节点即工具执行过程，则双方需要的是同一个 key**：`(strategyID, toolStepID)`。分期做会先造一个 `agent_id` 维度、下轮再废弃它。

∴ 融合点很明确：**B 提供节点的来源（真实过程），A 提供节点的可改属性（metadata patch），归因 key 从 agent 降到过程**。三件事做在一起才是一个闭环，拆开做两次都是半环。

***

## 6. 附录：已排除的"扁平化重写执行体为预定图"及其理由

因您曾倾向让"运行时执行体真按 DAG 分步推进"，此处明确排除理由：

- 若把 ReAct **每一轮**硬编码成 DAG 节点且要求**运行时按图推进**，则：

  1. 强制给 LLM 画预定步骤 ── LLM 不按图走即语义错位，**削弱自主性**；
  2. `chatStepState` 需存全图进度 + 每节点状态 → schema 重写、yield 语义重定义 → 触碰 quantum/checkpoint/调度契约，风险与成本最高；
  3. 与您欣赏的"工具调用是 DAG 节点"相比，它表达的是"预定步骤"，不是"真实工具轨迹"，反而不够自然。

- 结论：**以 B（旁路轨迹投影）替代它**，既保留"工具调用成节点"，又不动 ReAct。

***

## 7. 定案与已消解的待定项

原 §7 的两个待定项现已消解：

1. ~~选 A / B / A→B 分期~~ → **选 C（融合）**，理由见 §5。
2. 原"若 B 才需要决定"的四项，在 C 中全部成为**必做**且已定口径：

| 原待定项            | C 的定案                                                                                 |
| --------------- | ------------------------------------------------------------------------------------- |
| 边/血缘推断粒度        | **不做启发式血缘**。边只表达"同一 session 内的执行先后"（由事件的 round + seq 得出），不声称语义依赖 —— 近似边不作为 patch 依据。  |
| 作动回灌通道          | 节点 `Metadata`（`budget`/`prior`/`enabled`），经 §8.5 的 `Payload["tools"]→params` 装配落到执行体。 |
| 轨迹聚合 pattern 阈值 | 按 `toolStepID = toolName + argShape` 聚合（不按单条轨迹），阈值 = 最少样本数 `N`（默认给保守值，配置可调）。          |
| checkpoint 1→2  | **不升版**（§8.7 已核实：走事件日志投影，零 checkpoint 改动）。                                            |

***

## 8. 用户 review 修正（2026-09-04，全部经代码核实）

> 上表初稿对方案 A 的"易交付、低风险"判断**被证伪**。以下修正逐条对应初稿的错误。

### 8.1 方案 A 的作动通道实际是断的（初稿 §2.3 判断错误）

初稿说 A"基本无需新算子"。核实否定：

- 进化快照单位是 `engine.DAG`，节点类型 `DAGNode{StepID, InDegree, OutDegree}`（`workflow/engine/types.go:170`）——**不含 Metadata**。

- `WorkflowDiffer.Diff` 只比 Nodes 增删 + Edges 增删（`evolution/diff/workflow_differ.go`），metadata-only 改动产 0 个 patch。

- `DAGPatchExecutor` 仅支持 Insert/Remove/Replace/AddEdge/RemoveEdge（`dag_patcher.go:108`），无 metadata 算子。

- `mutateReplaceNode` 新建 Step 只拷 `ID/Name/AgentType/Input/DependsOn`（`workflow_genome.go`），会**抹掉 Metadata**。

∴ A 真实工作量 = 新增 `PatchSetNodeMetadata` + 扩 `DAGNode` 快照 + 修 genome 变异保留 metadata，**不是接线**。

### 8.2 生产路径上 `mutateTool` 是死代码（初稿"已存在"误导）

`buildMutator` 只传 `WithPromptPool`/`WithSeed`（`genome_wiring_system.go:187-195`），`SystemConfig` 无 ToolPool 字段；`WithToolPool` 唯一非测试调用在 `api/evolution`（公共 SDK，非 serve 路径）。∴ `hasTool := len(toolPool)>0` 恒 false → 工具变异永不选中；即便选中，len≤1 直接返回 clone（no-op）。

### 8.3 唯一活着的工具变异路径会清空 LLM 工具列表（现存 bug，非 Y.1 引入）

`guidedMutateTool`（`EnableExperienceGuidedMutation` 默认 true，`bootstrap_steps.go:313` 已接 GuidanceProvider）的词表来自 `extractToolNames`，`knownTools = {search,read,write,...}`（`experience_hints.go:160`），与实际注册工具名（`web_search/calculator/json_tools/...`）**零交集**。后果链：`Params["tools"]="search"` → 过滤后 `llmTools` 空 → 但 `chatAvailable` 用未过滤 schemas 判断（`chat_cognition.go:305`）照常进 Chat，模型拿零工具。**已修复**（见 §8.6 guard）。

### 8.4 ActiveStrategy 是全局单例，A 的"该 agent"语义不成立

`PGStrategyStore.GetActive` 是 `WHERE is_active=TRUE ... LIMIT 1`（无 agent 维度），改 `Params["tools"]` 影响**所有 agent**。初稿 §2.2"下一轮该 agent 工具白名单变"错误。per-agent 必须走节点 Metadata，而那条路被 §8.1 堵住。

### 8.5 装配缺口比初稿 §7.3 暗示的大

链路已通一半：`Step.Metadata → ProjectStep 合入 PlanStep.Payload（projection.go:63-65）→ CheckpointEnvelope.Payload`。但**无任何执行体从 task.Payload 读 tools**——`ParamKeyTools` 全仓只在 strategy.go 出现。缺的是 `renderPromptAndParams` 里 merge `task.Payload["tools"]` 进 params，并定义节点 vs 全局策略优先级。

### 8.6 修复与重新排序（已按此执行）

按用户建议优先级落地：

1. **✅ 已修**：空白名单 guard——过滤后 `len(filtered)==0` 时回退全量而非留空（`chat_cognition.go` + `sub/executor.go` 两处），加 warn 日志 + 回归测试 `TestToolWhitelistZeroIntersectionFallsBackToFullSet`。
2. **✅ 已修（C5）**：`Payload["tools"]→params` 装配与优先级已定（`agents.MergeNodeParams`，节点 Metadata > 全局策略 Params）；第三条执行体 `agentloop/engine.go` 已接白名单（`Request.ToolWhitelist`）。
3. **✅ 已修**：词表对齐分两层落地。变异侧 `extractToolNames` 的词表换成 `registeredToolAliases`（keyword → 真实注册名，§10.1），不再写出零交集白名单；护栏侧 `ValidateToolSet` 增第三条规则 `tool_set_unknown_name`（`WithKnownTools`），名字未注册即 Critical + `ShouldStop`——理由正是"名字全错的白名单会被运行期零交集 guard 兜成全量放开"，即静默变成最宽策略而非最窄。边界：`WithKnownTools` 需运营显式提供词表（`service.GuardrailsConfig.KnownTools`），留空则该规则不生效，与 `tool_weight` 默认 0 同属"出厂惰性闸门"。
4. **✅ 已修（C3）**：工具级 fitness 维度已落地——`tool_call` 证据带 `tool_step_id`，`WindowToolStep` 按 `(strategyID, toolStepID)` 读出，`Weights.ToolCall` 为默认 0 的显式闸门。
5. **✅ 已修（C6）**：`EvolutionGuardrails` 增 `ValidateToolSet`（`WithMaxToolsEnabled` 上界 + `WithRequireAnyTool` 零工具校验），并接进 `dream_cycle.findWinner` 选择路径——越界候选在 arena 测试前被拒、不消耗 arena 运行；全部候选被拒时返回 `ErrAllCandidatesRejected`（`Run` 视作正常空转）。计数口径与执行体共用 `agents.ToolNamesFromParams` 单一解析，保证"护栏数的工具"就是"LLM 看得到的工具"。

### 8.7 方案 B 的 checkpoint 门槛被初稿高估

初稿 §3.3.1 说 B 需 checkpoint schema 1→2。**否定**：`EventToolCallStarted/Completed` 已带 `tool_name+tool_call_id`，且在每轮 yield 前落到 EventStore（`chat_cognition.go:358-375`）——轨迹可从事件日志重建，**零 checkpoint 改动**，只需给两事件补 `round` 字段（2 行）。

（若未来真要升版，还有两坑：`decodeChatStepState` 用严格 `!=`（`chat_cognition.go:289`），bump 会导致在飞 v1 checkpoint resume 失败，须先改 `>` 语义；且 `stepSchemaVersion` 有**两处独立定义**——`sub/executor.go:40` 与 `chat_cognition.go:47`。）

### 8.8 修正后的结论

- 初稿 §5"A 今天可交付、风险极低"**不成立**：A 需 metadata 算子 + 快照扩展 + genome 保留 + 词表对齐 + guard。

- 优先序改为：**先修现存 bug（guard+词表）→ 明确 Payload 装配 + 补 agentloop → 落工具级 fitness → 再定 A/B**。

- A（若选）必须承认需 metadata 维度 patch/diff/快照扩展；B（若选）走**事件日志投影**，**不动 checkpoint schema**。

- 无论 A/B，**先落工具级 fitness 维度**，否则作动无法被 GA 选择。

***

## 9. 第二轮 review 修正（2026-09-04，全部经代码核实）

> §8 纠正了"乐观但乐观在有检测"。本轮 review 更深一层，指出"**已落地代码里作动与选择所依赖的前置条件仍未验证**"。

### 9.1 所有待办的前置条件缺失：fitness 传导未经验证（最优先）

§8.6 第 4 条把"工具级 fitness 维度"列为 A/B 成立前提——**必要但不充分**。即便加了工具维度，若"基因差异 → score 差异"这条链路断裂，进化仍会在选择环节看到同一分。

**核实的机制修正**：`StrategyHash`（`scoring/hash.go:40`）用 `fmt.Fprintf("%s=%v", 每个 Params 键值)` ——它**已包含** **`Params["tools"]`**，所以工具变异的子代 hash ≠ 父代 hash，**不会误命中父代 ScoreCache 条目**（最初担心的 Clone 传 ScoreCache 那条，经核实 `ScoreCache` 以 hash 为 key 且 hash 含 tools，不成立）。

**但本质论点成立且最关键**：ScoreCache 的**预填充路径**（`genome_wiring_run.go:141-157` 的 `batchScorer`，N 个 agent 合并成 LLM 批量调用）——批量 scorer 是否真的把"不同 `Params["tools"]` 的工具调用成败差异"反映进分数，**无人验证**。如果 batch scorer 只用 prompt + 数值参数打分、忽略工具特征，那么工具变异的 fitness 差异**仍到不了选择环节**（尽管 ScoreCache key 本身无问题）。

∴ **必须把一条端到端断言排在 A/B 所有工作之前**：

> "两个仅 `Params["tools"]` 不同的策略，在工具级 fitness 维度开启后，必须得到**不同的聚合 fitness**。"

这条断言现在**没有任何测试**。它也是 §8.8"先落工具级 fitness"的前提——不先验证传导，fitness 维度做了也可能白做。

### 9.2 §8.4 的对偶缺口：归因侧同样全局

§8.4 只写了作动侧（`GetActive` 全局单例），**归因侧同样全局**：`ChannelFeedbackRecorder.write` 用 `r.activeID()` 归因（`channel_feedback.go:324`），所有 `tool_call` 证据都记到**同一个** `strategyID`。

∴ 即便 per-agent 作动通过节点 Metadata 打通了，GA 在**证据侧**仍分不出"是哪个 agent 的哪个工具集贡献的"——两个 agent 用不同工具集，`tool_call` 证据却合在同一策略下。**A 和 B 都受制于此**（B 的轨迹投影同样需要 agent 维度的证据才能学到"哪条轨迹好"）。

对策：`activeID()` 需升级为 `(strategyID, agentID)` 双 key，或在 evidence payload 增加 `agent_id` lively 并让 aggregator 按 (策略, agent) scope 读取。文档 §7 之前未提此维度。

### 9.3 §8.7 对 B 的工作量估算偏低

§8.7 说方案 B"补 round 字段 2 行"即可重建轨迹——**低估了两件事**：

1. **成败信号不在事件里**：`EventToolCallCompleted` 的 payload（`chat_cognition.go:381`、`sub/executor.go:936`）只有 `agent_id/tool_name/tool_call_id`，**无 success/error/output**。执行失败时 `err` 只进 `result = fmt.Sprintf("error: %s", ...)` 塞回 messages，**没进事件**。而 §3.2 指望"轨迹 DAG 节点失败率作 fitness 维度"——**这个信号从事件日志取不到**，要先给 `EventToolCallCompleted` 补 `success/error` 字段。
2. **eventStore 接线是隐含前提**：`emitEvent` 在 `eventStore == nil` 时静默 no-op。"从事件日志投影"隐含一个**未声明前提**：eventStore 必须已接线。未接线则轨迹投影恒空。

∴ B 实际需：补 `success/error` 事件字段（不只 round）+ 确认/强制 eventStore 接线，才能支撑"节点失败率作 fitness"。

### 9.4 三条执行体的事件契约不同构（文档此前按同构处理）

文档 §8.6 第 2 条只说"补 agentloop 第三条执行体"——实际问题不是少接一条，是**三条执行体的事件 payload 形状本就不一致**：

- `agentloop/engine.go:437`（sdk 路径）：带 `tool/args/result/success`——**最全**。

- `sub/executor.go:936`、`chat_cognition.go:381`（peer 两条生产路径）：只有 `agent_id/tool_name/tool_call_id`——**无成败**，是最简。

- `internal/workflow/graph/node.go:118-211`（graph 执行器）：另一种 payload。

而 `ares_archive/extract.go` 的 `extractVerdict/extractFileChanges`（`:107/:226`）读了 `payload["output"]`（`:472`），该字段**只有 sdk/callback\_bridge 路径提供** → 这些提取器对 **peer 事件静默返回空**。

∴ B 的投影层**必须先统一三条执行体的事件契约**（至少 peer 两条与 sdk 对齐到含 results/success），否则按 agentloop 的形状投影会漏 peer 数据。这比"补一条"工程量大。

### 9.5 方案缺少验收信号

A 和 B 都没定义"**怎么知道它真的 работает**"。既然 fitness 传导这种核心链路能**悄悄坏掉一整轮而所有测试照绿**（§9.1），方案文档应自带一条端到端断言。

最小验收信号（无论 A/B）：

> "两个**仅** **`Params["tools"]`** **不同**的策略，开启工具通道后，必须产生**不同的 fitness**（归因到各自策略），且该差异可由 `RuntimeFitnessAggregator` 读到。"

这条同时覆盖 §9.1（传导）、§9.2（归因双 key）、§9.3（成败入事件）——它是 Y.1 是否有意义的最终判定。

### 9.6 次要：两个 store 对"活跃策略"的判定不一致

- `pg_strategy_store.go:104`：`ORDER BY created_at DESC`。

- `strategy_repository.go:62`：`ORDER BY version DESC`。

多活跃行时二者给出不同答案。正常路径下 `SetActive` 先 deactivate all，不会触发；但作为一致性欠账应记录，避免未来依赖时踩坑。

***

## 10. 本轮落地状态（2026-09-04，已编译 + 测试通过）

### 10.1 已完成

| 项                  | 内容                                                                                                                                                                                                                                                                 | 位置                                                                                                                    |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| 空白名单 guard         | 白名单与注册工具零交集时回退全量，避免把空工具表交给 LLM                                                                                                                                                                                                                                     | `agentfabric/chat_cognition.go`、`agents/sub/executor.go` + 回归测试 `TestToolWhitelistZeroIntersectionFallsBackToFullSet` |
| Clone 不继承 hash 缓存  | `Clone()` 是所有变异路径入口，继承 hash 会让子代命中父代 ScoreCache 条目、拿到父代分数 → 选择压力归零                                                                                                                                                                                                 | `mutation/types.go`                                                                                                   |
| EvidenceKey 整型不再丢失 | `numericParam` 覆盖 int/uint/float 全族；此前裸 `.(float64)` 断言会丢掉 `top_k`/`max_steps`/`memory_limit`，仅这些维度不同的策略会塌到同一 EvidenceKey                                                                                                                                          | `mutation/types.go` + 测试                                                                                              |
| **词表对齐（§8.6-3）**   | `extractToolNames` 的词表从裸动词 `{search,read,write,exec,...}` 换成 `registeredToolAliases`：keyword → **真实注册名**（`web_search`/`file_tools`/`calculator`/`json_tools`/…），去重且顺序确定。此前零交集导致每次 guided tool 变异都写出匹配不到任何工具的白名单                                                    | `ares_evolution/experience_hints.go` + 测试（含"别名目标必须是注册名"不变式测试）                                                         |
| **§9.5 端到端验收断言**   | 新增 `tool_dimension_transmission_test.go`，按四链断言"仅 `Params["tools"]` 不同 → fitness 不同"：①`StrategyHash` 分得开（否则命中兄弟 ScoreCache）②`ComputeEvidenceKey` 分得开（否则证据合并）③`Weights.ToolCall>0` 通道已武装 ④`Window` 真读到差异；配套 `TestToolDimension_UnarmedChannelIsInert` 说明默认 0 权重是刻意闸门 | `ares_evolution/tool_dimension_transmission_test.go`                                                                  |

§9.1 的核心结论由此被测试固定：**传导链现在是通的，但第 ③ 链（`Weights.ToolCall`）默认 0**——工具维度出厂即惰性，需运营显式给权重才进入选择。这是发布措辞必须写明的一条。

### 10.2 仍未闭环（按优先级）

> **状态（2026-09-04 执行完成）**：下列 1–4 全部已落地并测试覆盖，方案 C 的 C1–C7 各阶段验收全数通过，`make check` 全绿。详见 §12「执行记录」。

1. ~~`Payload["tools"] → params`~~ ~~装配 + 节点 vs 全局策略优先级（§8.5），并补~~ ~~`agentloop/engine.go`~~ ~~第三条执行体。~~ ✅ 已落地（C5）。
2. ~~归因双 key（§9.2）——在 C 中定为~~ ~~`(strategyID, toolStepID)`，不做~~ ~~`(strategy, agent)`（§5-3）。~~ ✅ 已落地（C3）。
3. ~~`EvolutionGuardrails`~~ ~~增加工具集 allowlist 上界（§8.6-5）。~~ ✅ 已落地（C6）。
4. ~~统一三条执行体事件契约、补~~ ~~`EventToolCallCompleted`~~ ~~的~~ ~~`success/error`（§9.3、§9.4）——在 C 中是必做前置（投影层的输入），不再是"若选 B"。~~ ✅ 已落地（C1）。

***

## 11. 方案 C 可执行计划（定案，按依赖排序）

### 11.0 节点定义（唯一需要新造的概念）

```
ToolStep  ——  一次“工具执行过程”，DAG 的节点
  toolStepID = toolName + "#" + argShape      // 聚合键，不含具体参数值
  节点属性（Metadata，进化可 patch）:
     enabled  bool     // 该过程是否还允许发生
     budget   int       // 该过程在一个 session 内的最大次数
     prior    float64   // 注入提示词的偏好强度（不禁用，只倾向）
  边: 同一 session 内的执行先后（round, seq），不声称语义依赖（§7）
```

`argShape` = 入参键集合的规范化串（排序、只取键名不取值），这样"同一个工具的同一种用法"聚合成一个节点，避免 §3.3-4 的轨迹碎片化。

### 11.1 阶段划分与验收（2026-09-04 全部执行完成，验收通过）

| 阶段     | 交付                                                                                                                                                | 依赖           | 验收（可执行断言）                                                                                                        | 状态                                                                                                             |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ | ---------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| **C1** | **事件契约统一**：`EventToolCallCompleted` 三条执行体（`chat_cognition.go` / `sub/executor.go` / `agentloop/engine.go`）统一带 `round/seq/success/error/arg_shape` | 无            | 三条执行体各跑一次工具失败，事件 payload 的 `success=false` 与 `error` 均非空；断言三处 payload 键集合相同                                      | ✅ `ares_events/tool_events.go` + 三执行体接线 + `tool_events_test.go`                                                |
| **C2** | **投影层**：从 EventStore 把事件流投影成 `ToolStep` 节点 + 先后边，按 `toolStepID` 聚合（含 `N` 次最少样本阈值）                                                                 | C1           | 给定一段人造事件流，投影出的节点数 = 不同 `toolStepID` 数；同 `toolStepID` 的多次调用聚合成 1 节点且 `count/success_rate` 正确                      | ✅ 新包 `internal/toolprojection` + `projection_test.go`                                                          |
| **C3** | **过程级归因**：`ChannelFeedbackRecorder` 的 `tool_call` 证据带 `tool_step_id`，aggregator 按 `(strategyID, toolStepID)` scope                                | C1           | 同一策略下两个不同 `toolStepID` 的成败，产生两条可分辨的证据；`Window` 能按 toolStepID 读出不同值                                               | ✅ `feedback.ToolCallOutcome.ToolStepID` + `channel_feedback.go` + `WindowToolStep` + 测试                        |
| **C4** | **作动面**：`DAGNode` 快照带 Metadata + `PatchSetNodeMetadata` 算子 + `WorkflowDiffer` 出 metadata patch + `mutateReplaceNode` 保留 Metadata（§8.1 四处）         | 无（可并行 C1–C3） | 仅改一个节点 Metadata 的 parent→child，`generateDiffPatches` 产出 **1** 个 patch（当前为 0）；patch 应用后 live DAG 上该节点 Metadata 已变 | ✅ `DAGNode.Metadata` + `MutableDAG.SetNodeMetadata` + `PatchSetNodeMetadata` + differ + `wfOpSetMetadata` + 测试 |
| **C5** | **装配回灌**：`Payload["tools"]/budget/prior → params` 合入 `renderPromptAndParams`，定义"节点 Metadata > 全局策略 Params"优先级；三条执行体都读（§8.5、§10.2-1）               | C4           | 节点 Metadata 给出的白名单**覆盖**全局策略白名单；budget 用尽后该工具从 schema 层撤下                                                       | ✅ `agents.MergeNodeParams` + `ToolBudgetFromParams`/`ToolAllowedByBudget`/`ApplyPriorHint` + 两执行体 renderPromptAndParams 与预算门接线（`ToolUses` 随 checkpoint 持久化）+ `agentloop.Request.ToolWhitelist`（第三执行体，仅白名单）+ 测试      |
| **C6** | **护栏**：`EvolutionGuardrails` 增工具集 allowlist 上界（§8.6-5）；`enabled=false` 不得导致零工具（复用 §10.1 的 guard 语义）                                               | C5           | 变异写出越界工具集时被 guardrail 拒绝并计数                                                                                      | ✅ `ValidateToolSet`（上界 / 零工具 / 未注册名三规则）+ `WithMaxToolsEnabled`/`WithRequireAnyTool`/`WithKnownTools` + `ErrCodeInvalidToolSet` + dream\_cycle 与 genome 两条晋升路径接线 + 测试                |
| **C7** | **端到端闭环断言**（Y.1 的最终判定，§9.5 的过程级版本）                                                                                                                | C1–C6        | 仅 `ToolStep.Metadata` 不同的基因 → 四链全部分得开，且 GA 能 promote 成功率高的那一侧（需 `tool_weight>0`）                                 | ✅ `TestToolStepMetadata_EndToEndClosedLoop`（经 `WindowToolStep` 分出高成功率侧）                                        |

### 11.2 关键约束（实施时不得违反）

- **不动 ReAct 自主**：`enabled/budget` 作用在 schema 过滤层（与 §10.1 guard 同一位置），`prior` 只进提示词；**不得**在 `CallTool` 时拦截，也不得给 LLM 预定步骤（§6 排除项仍然有效）。

- **不动 checkpoint schema**：投影只读 EventStore（§8.7）。若发现必须带状态，先回到本文档改设计，不得就地 bump（`decodeChatStepState` 严格 `!=` + 两处 `stepSchemaVersion` 定义，§8.7 括号）。

- **eventStore 必须接线**：未接线时投影恒空（§9.3-2）。C2 需显式检查并在未接线时 warn，而非静默产出空图。

- **默认关闭**：`tool_weight` 默认 0 的闸门保持（§10.1）；C1–C6 全部落地后行为与今天一致，只有显式给权重才进入选择。

- **近似边不作为 patch 依据**：边只用于展示与顺序统计（§7）。

### 11.3 发布措辞（C 全部完成前）

在 C7 通过前，**不可**写"进化作用于单 agent 内部执行过程"。C1–C3 完成可写"单 agent 内部工具执行过程已可观测并归因到进化判决（默认关闭）"；C4–C6 完成才可加"且可被进化作动"。协作维度的禁止措辞不变（`AGENT_OS_CLOSURE_DEV_PLAN.md` N-11/N-12）。

***

## 12. 执行记录（2026-09-04，C1–C7 全部落地）

> 本段记录方案 C 各阶段的实际交付与验证，作为"已定案并执行"的唯一依据。全部改动遵循 `plan/rules/code_rules_v2.md`，`make check`、`go test -race` 全绿。

### 12.1 交付清单（代码位置）

| 阶段 | 代码位置                                                                                                                                                                                                                                                 | 说明                                                                                                                                                                                                 |
| -- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| C1 | `internal/ares_events/tool_events.go`（新）`ToolCompletedPayload`/`ToolArgShape`/统一 key                                                                                                                                                                 | 三条 ReAct 执行体（`chat_cognition.go`/`sub/executor.go`/`agentloop/engine.go`）的 `EventToolCallCompleted` 统一携带 round/seq/success/error/arg\_shape；`EventToolCallStarted` 同步补 round/seq。                  |
| C2 | `internal/toolprojection/`（新包）+ `Projector`（`projector.go`）                                                                                                                                                                                          | 纯读投影：事件流 → `ToolStep` 节点（按 toolStepID 聚合，`N` 最少样本阈值）+ 同 session 先后边。`Projector.Run` 把每步成功率写成 `tool_call` fitness evidence（带 `tool_step_id`），打通 C2→C3（投影层此前无生产消费点）。                                 |
| C3 | `internal/feedback` `ToolCallOutcome.ToolStepID`、`sub/tool_observer.go`、`ares_evolution/channel_feedback.go`、`fitness_aggregator.go` `WindowToolStep`                                                                                                | process 级归因：tool\_call 证据带 `tool_step_id`；`WindowToolStep` 按 `toolStepID`（可空 strategyID）读取。生产 tool\_call evidence 真实写入端是 `ChannelFeedbackRecorder`（经 binder observer），投影层是事件流审计/重建路径。              |
| C4 | `workflow/engine/types.go` `DAGNode.Metadata`、`mutable_dag.go` `SetNodeMetadata`、`graph_events.go` `ChangeSetNodeMetadata`、`dag_patcher.go` `PatchSetNodeMetadata`、`evolution/diff/workflow_differ.go`、`genome/workflow_genome.go` `wfOpSetMetadata` | metadata 作动面四件套：快照带 Metadata、patch 算子、differ 出 metadata patch、replace/home 保留 Metadata。                                                                                                            |
| C5 | `agents/strategy.go` `MergeNodeParams` + `ParamKeyBudget/ParamKeyPrior` + `ToolBudgetFromParams`/`ToolAllowedByBudget`/`PriorHintFromParams`/`ApplyPriorHint`、`chat_cognition.go`/`sub/executor.go` renderPromptAndParams 接线 + 预算门 + `chatStepState.ToolUses`、`agentloop/engine.go` `Request.ToolWhitelist`                                                                 | 装配回灌：节点级 tools/budget/prior 覆盖全局策略；补齐第三条执行体（agentloop 白名单）。budget 语义按 §11.2 落在 schema 过滤层（用尽即撤下该工具的 schema，不在 `CallTool` 拦截），计数发生在执行**前**（失败调用同样消耗预算），并随 checkpoint 持久化 `ToolUses`（否则 resume 会把预算清零）；prior 只前置进提示词，不改工具集。语义实现只有一份，两执行体共用。                                                                                         |
| C6 | `ares_evolution/guardrails.go` `ValidateToolSet` + `WithMaxToolsEnabled`/`WithRequireAnyTool`/`WithKnownTools` + `ErrCodeInvalidToolSet`；接线：`dream_cycle.go findWinner` 与 `genome_wiring_run.go` `toolSetRejected`（`Run` 的 lifecycle submit 前 + `deployBestStrategy` 直发前）；解析收口：`agents.ToolNamesFromParams`                                                          | 三条规则：`tool_set_upper_bound`（数量上界）、`tool_set_empty`（零工具）、`tool_set_unknown_name`（§8.6-3 词表，名字未注册即拒）。接进两条晋升路径：dream\_cycle 选择路径（越界候选在 arena 测试前被拒、不消耗 arena 运行；全拒 → `ErrAllCandidatesRejected`）与 genome 适配器的两个出口。护栏计数与执行体过滤共用同一解析（去空 + 去重），否则 `"a,a"`/`"a,b,"` 这类变异产物会让护栏数到 2 而执行体只放开 1，把合规候选误判越界。 |
| C7 | `ares_evolution/tool_dimension_transmission_test.go` `TestToolStepMetadata_EndToEndClosedLoop`                                                                                                                                                       | 真实端到端：C1 契约事件 → eventStore → `Projector.Run` → evidence store → `WindowToolStep` 分出高成功率侧（不再手造 evidence）。                                                                                           |

### 12.2 归因粒度说明（C3 的 key 落地口径）

按 §5-3 / §9.2 定案，归因 key 取 **`(strategyID, toolStepID)`**，不做 `(strategy, agent)`——同进程/同策略下两个不同的 "同一工具的不同用法" 因此可分。`toolStepID = toolName#argShape`，`argShape` 由 `ares_events.ToolArgShape`（JSON 原始串）与 `sub.toolStepID`（已解码 map）两侧分别计算，排序键名、忽略取值，两边产出同一 key。

### 12.3 验证

- `go build ./...`、`go vet ./...`、`gofmt -l .`（无输出）、`golangci-lint run ./...`（0 issues）、`make check`（exit 0）。

- `go test -race` 覆盖：`toolprojection`/`ares_events`/`ares_evolution`/`workflow/engine`/`evolution/{diff,genome,patch}`/`agents`/`agentfabric`/`agentloop`/`sdk`。

- 关键测试：`testToolEvents`（C1）、`projection_test.go`（C2）、`TestAggregator_WindowToolStep*`（C3）、`TestWorkflowDiffer_MetadataOnlyChangeEmitsOnePatch` 等（C4）、`TestMergeNodeParams`/`TestEngine_ToolWhitelist*`（C5 装配与第三执行体）、`strategy_budget_prior_test.go` 的 `TestToolBudgetFromParams`/`TestToolAllowedByBudget`/`TestToolAllowedByBudgetExactCallCount`/`TestPriorHintFromParams`/`TestApplyPriorHint`/`TestPriorDoesNotAffectToolSet`（C5 语义单元）、`sub/executor_tool_budget_test.go` 与 `agentfabric/chat_cognition_tool_budget_test.go` 各 5 例（C5 接线：撤下用尽工具、失败调用仍消耗预算、全用尽回退全量、无预算=无限、与白名单叠加；后者另含 `TestChatCognitionBudgetSurvivesCheckpointRoundTrip` 经 `decodeChatStepState` 往返断言 `ToolUses` 存活）、`TestValidateToolSet` + `TestValidateToolSetKnownTools`（§8.6-3 词表：known/unknown/全 unknown/无词表/空白词表/trim/空集跳过/两规则并发）+ `dream_cycle_toolset_guard_test.go` 的 `TestFindWinner_ToolSetGuardRejectsBeforeArena`/`_AllCandidatesJailedByToolSetGuard`/`_ToolSetGuardCountsSameNamesAsExecutor`/`_NoToolsWhitelistNotRejectedByDefault`/`_RequireAnyToolJailsEmptyWhitelist` + `genome_wiring_toolset_test.go` 的 `TestGenomeWinnerToolSetGuardrail`（越界不部署/合规部署/未注册名不部署/无护栏不变/无白名单可部署）与 `TestToolSetRejectedGuard`（nil strategy、nil guardrails、`"a,a,"` 解析为 1 个名不误杀）（C6：单元 + 两条晋升路径接线，含"护栏与执行体同一计数"与"无白名单不误杀"两条防回归）、`TestToolNamesFromParams`/`TestToolNamesAndWhitelistAgree`（解析收口）、`TestExtractToolNames_AliasTargetsAreRegisteredNames`（词表不变式）、`TestToolStepMetadata_EndToEndClosedLoop`（C7）。

### 12.4 发布措辞（C1–C7 完成后）

C7 已通过，原 §11.3 的禁用措辞到期。可写：**"进化已可作用于单 agent 内部工具执行过程（默认关闭，需显式给** **`tool_weight`）——事件契约已统一、投影可观测、过程级归因可读、metadata 可作动、护栏可校验"**。协作维度的禁止措辞（`AGENT_OS_CLOSURE_DEV_PLAN.md` N-11/N-12）不变：协作 ACT 仍未落地（`ask_agent` 已落地，但影子 deny-list 扩到协作仍在排期）。

### 12.5 遗留与边界（如实声明，不做掩盖）

1. **✅ `Projector`** **的生产触发点已接线（见 §12.6）**：`internal/ares_bootstrap/tool_projection_worker.go` 把 `Projector.Run` 接进 serve 的后台 goroutine 组，由 `evolution.tool_projection`（默认 `enabled: false`）控制周期与最少样本阈值。它与 `ChannelFeedbackRecorder`（binder observer）**不重复**：后者按次写"这次调用成不成"，前者按窗口投影"这种用法成不成"（`tool_step_id` 维度的唯一生产者）。**边界**：默认关闭，未显式开启时行为与接线前一致；未接 eventStore/evidenceStore 时 worker 拒绝启动并 warn（§9.3-2），不写空图。
2. **budget/prior 消费语义已落地，但只覆盖两条执行体**：`agents.ToolBudgetFromParams`/`ToolAllowedByBudget` 把 "budget 用尽后该工具不再出现" 落在 **schema 过滤层**（与 §10.1 guard 同一位置，未在 `CallTool` 拦截，符合 §11.2），`PriorHintFromParams`/`ApplyPriorHint` 让 `prior` **只**前置进提示词、不动工具集；两处口径共用同一份实现。计数在执行**前**发生，因此失败调用同样消耗预算；`ToolUses` 随 checkpoint 持久化（pre-budget checkpoint 解码为 nil = "尚未使用"），resume 不会把预算清零；全部工具用尽时沿用 "永不广告零工具" 回退全量并 warn。**边界**：接线只覆盖 `sub/executor.go` 与 `agentfabric/chat_cognition.go`；第三条执行体 `agentloop/engine.go` 仍只消费 `Request.ToolWhitelist`，不消费 budget/prior，且其唯一生产构造点 `sdk/agent.go` 未填充 `ToolWhitelist`（该路径没有策略 params 来源），所以 agentloop 上的工具旋钮目前是"有能力、未接源"。
3. **✅ C6 接线已覆盖两条晋升路径 + 词表规则已加 + 词表真源已配置化**：越界候选在 `dream_cycle.findWinner` 被拒且有测试断言**被拒候选不进 arena**；genome 适配器路径新增 `toolSetRejected`，接进 `Run`（lifecycle submit 前）与 `deployBestStrategy`（直发前）两个出口，被拒时记护栏事件 + `metrics.RecordEvolutionGuardrail`。护栏现在校验数量上界、零工具、**以及工具名是否真实注册**（`tool_set_unknown_name`）。**工具词表单一真源 = `evolution.tool_pool` / `evolution.guardrails.known_tools` yaml 配置**（`ares_config.EvolutionConfig`），经 `applyGATuning`/`buildEvolutionGuardrails` 下发；`buildMutator` 接 `WithToolPool`，使 elite/random mutation 真正能凭配置产白名单（此前 `WithToolPool` 在 serve 是死配置）。运行期零工具回退 guard（chat\_cognition/sub）仍作兜底。**边界**：`known_tools` 留空则 `tool_set_unknown_name` 静默不生效（配置即真源，未配即不校）。
4. **协作 ACT**：仍为开环（见 AGENT\_OS N-11/N-12）。

这些是本次执行如实记录，不是"全部闭环"的夸大。

***

## 13. 执行记录（2026-09-05，§12.5-1 遗留项收口：投影器生产触发点）

> 本段只处理 §12.5 第 1 条遗留项："`Projector` 未接进 serve 生命周期循环"。C1–C7 的结论不变。

### 13.1 为什么需要它（不是重复接线）

`tool_step` 这个 fitness 维度此前**没有生产者**：`WindowToolStep` 按 `tool_step_id` 读取，而线上唯一写 `tool_call` evidence 的 `ChannelFeedbackRecorder` 走的是"每次调用一条记录"的路径。两者回答的是不同问题：

| | `ChannelFeedbackRecorder`（既有） | 投影 worker（本次） |
| --- | --- | --- |
| 触发 | 每次工具调用（binder observer） | 周期 tick |
| 单位 | 一次调用 | 一个窗口内同 `toolStepID` 的全部调用 |
| 回答 | "这次调用成不成" | "这**种用法**成不成"（`tool#argShape` 的成功率） |

成功率是**窗口统计量**，单条事件算不出来（一次调用的成功率只能是 0 或 1，没有统计意义）——所以只能是周期 worker，不能改成事件订阅。二者可同时开启，互不覆盖。

### 13.2 交付

| 位置 | 内容 |
| --- | --- |
| `internal/ares_bootstrap/tool_projection_worker.go`（新） | `startToolProjectionWorker`（组装 + 三种拒绝启动条件）+ `runToolProjectionLoop`（周期 + 读游标） |
| `internal/ares_bootstrap/bootstrap_steps.go` | 在 `startChannelFeedback` 之后接线 |
| `internal/ares_config` `ToolProjectionConfig` + 默认值 + 校验 | `evolution.tool_projection.{enabled,interval,min_samples}` |
| `internal/toolprojection/projection.go` `Options.Since` | 让投影支持增量读（`ProjectFromSource` 透传到 `ReadOptions.Since`） |
| `configs/ares.yaml` | 注释块说明它与 `channel_feedback.tool_enabled` 的分工 |

### 13.3 三个必须这样做的决定（否则是错的，不是风格问题）

1. **游标从"启动时刻"开始，不从日志开头**：若首 tick 读全量历史，会把**历次策略**产生的工具行为算到启动时刻恰好激活的那个策略头上——与 active-strategy 解析器在其它通道上防的是同一类错误归因。
2. **游标只在成功后前移**：失败 tick 保留窗口，下一 tick 重投影。代价是可能重复写入失败前已写的部分证据（轻微拉低/抬高该步得分）；反向选择（跳过窗口）会**永久丢掉一窗口的工具失败信号**，那正是这条链路存在的意义。故取前者。
3. **窗口右界在读之前取快照**：否则投影期间落地的调用会被夹在"已读完"与"游标前移"之间整窗丢失。
4. **未接 eventStore 即拒绝启动**（§11.2 / §9.3-2 明文要求）：否则 worker 会在"看起来已接线"的状态下每 tick 写一张空图。

### 13.4 验证

- `go build ./...`、`go vet`（三包）、`read_lints` 无诊断。
- `go test -race -count=1`：`internal/toolprojection`、`internal/ares_config`、`internal/ares_bootstrap` 全绿。
- 新增测试：
  - `tool_projection_worker_test.go`：`TestToolProjectionLoop_CursorStartsAtNowAndAdvances`（断言首窗口不是零时间——即"不重放全史"这条防回归）、`_FailedRunKeepsCursor`、`_StopsOnContextCancel`、`TestStartToolProjectionWorker_RefusesWithoutStores`（冻结时钟，无 sleep）。
  - `projection_test.go`：`TestProjectFromSource_ForwardsSinceWindow`（窗口必须真的下推到 store，否则每 tick 重发已投影证据）、`_ZeroSinceReadsWholeLog`（保住按需审计读）。
  - `evolution_config_test.go`：`TestToolProjectionConfig_YAMLParsesAndDefaults`（缺省关闭、只开 `enabled` 也得到安全窗口、显式值生效、armed 时负 interval 被拒而 disabled 时不拦启动）。

### 13.5 发布措辞更新

§12.5-1 的限制（"只写可运行且链路已打通，不要写投影器已在运行"）到期，替换为：**"投影器已接入 serve 后台循环，默认关闭；开启后按 `evolution.tool_projection.interval` 周期产出 `tool_step` 维度证据"**。其余措辞（含协作 ACT 仍开环）不变。

***

## 取代声明（2026-09-05，仅追加，不改原文）

本文档方案 C 已被 `TOOL_DAG_MAINLINE_DESIGN.md` 取代：节点 = 一次工具执行；ReAct 消息循环取消，展开成 L2 执行图；L1/L2 同构 `engine.MutableDAG` 为全系统唯一图表示，不再有投影/影子/事后重建。C1/C3/C4/C5/C6 的产出物由主线直接继承（见主线 §10），**C2（`toolprojection` 投影层）随取代作废**——其 `tool_projection_worker` 接线与 §13.5 的发布措辞均不再作为目标状态。本文档保留为历史记录；后续实施以 `TOOL_DAG_MAINLINE_DESIGN.md` 为唯一依据。
