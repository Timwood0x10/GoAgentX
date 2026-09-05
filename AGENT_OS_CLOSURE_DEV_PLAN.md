# Agent OS 端到端闭环 — 开发计划

核实时间：2026-09-03。以下每条断点均已在代码中比对过行号，不是推测。

> **进度状态（2026-09-03 续接）**
>
> - ✅ 已落地：Step 1-6 全部（含 6.1 策略 ID 透传进补丁并 fail-fast；6.2 `deploymentStagingRuntime` 改持 `asm` 引用、`Evaluate` 内 `asm.Current()` 现取 baseline）。
>
> - ✅ 已落地：Step 7.1——`UpdateLiveDAG` 现在就地调用 `WorkflowGenome.SetDAG` 把基因组重指到 live DAG，并删掉旧的 "NOT updated here" 注释；新增回归测试 `TestUpdateLiveDAG_RepointsWorkflowGenome`；修掉 `GenomeReg == nil` 时的空指针。
>
> - ✅ 已落地：Step 7.2——7.2.1 字面 `generateDiffPatches` 同图断言（`ares_evolution/generate_diff_patches_workflow_test.go`，含正向 `TestGenerateDiffPatches_SameGraphNodeRefsResolveInLiveDAG` 与反证 `TestGenerateDiffPatches_CrossGraphMismatchIsDetected`，证明基因组读 live DAG、patch 节点 ID ⊆ live DAG ∪ 本次引入节点）；7.2.2「可观测变更」集成测试 `ares_bootstrap/deploy_live_dag_integration_test.go` 的 `TestDeployLiveDAG_ObservableTopologyChangeAndRollback`（Deploy 后 `runtime.GetAgentDAG(AgentDAGLiveKey)` 拓扑变化、`Rollback` 就地复原、同一指针）；7.2.3 F04 隔离由 `closure_contract_test.go` 覆盖并通过（`TestClosure_Ready_AllExecutorsBoundToLiveTargets`）。
>
> - ✅ 已落地：Step 7.3——非 serve 入口显式日志（N-4）：`bootstrap.go` 在注册合成图后、`cfg.Evolution.Enabled` 时输出 "evolution verdicts available but no live agent topology to act on"（`live_dag_registered=false` + 合成图 key），明确 serve 入口是唯一 live DAG 供给方。
>
> - ✅ 已落地：Step 4——真实执行 A/B（N-1，2026-09-03）：
>
>   - 调度器捕获：`kernelscheduler.Scheduler` 新增 `WithShadowExecutionHook`（`shadow.go`），`executeWithCandidates` 在量子成功收尾后把 `ToModelTask` 视图交给钩子（失败量子不采样；钩子契约「只入队不阻塞」）。
>
>   - 隔离执行：`ares_evolution/shadow_executor.go` 的 `ShadowExecutor` 同时实现 `ShadowExecutionHook`（结构化满足，零 import 回环）与 `ShadowExecutionFeeder`。`Feed` 对候选与活跃双臂在同一批缓冲任务副本上执行，双臂各写一条 `strategy_id==自身` + `shadow:true` 的 `strategy_shadow` fitness 证据；跑不起来的臂不造假分、不成对。证据走独立 source，rollback 窗口与 deployment staging 的活跃分不受影子记录污染。
>
>   - 副作用 deny-list：serve 侧 `cmd/ares/shadow_execution.go` 的 `shadowDenyBinder` 在接口层拦截一切工具调用（`ListTools` 返回空、`CallTool` 返回 `errShadowToolDenied`，生产 binder 全程零触达，测试用接口级 spy 断言计数 == 0）；影子 cognition 不接生产 EventStore。
>
>   - 判决回流：`ShadowSampler.SetExecutionFeeder`（`lifecycle.ShadowSampler()` 暴露）——`Prime` 先跑真实执行 A/B 并把配对结果 `RecordResult` 进 G2 判决，无产出时回退 replay 窗口（语义不变）。
>
>   - 配置：`evolution.shadow_execution: {enabled: false, sample_size: N}` 默认关闭（`ares_config` + `configs/ares.yaml`）；`wireShadowExecution` 在 evolution/影子执行/采样器任一缺失时显式 no-op 并留日志。
>
>   - 测试：`kernelscheduler/shadow_hook_test.go`（采样契约）、`ares_evolution/shadow_executor_test.go`（配对证据/采样条数/克隆隔离/防造假 guard）、`ares_evolution/shadow_sampler_construction_test.go`（构造路径，N-3 薄弱点）、`cmd/ares/shadow_execution_test.go`（deny-list spy 断言 + 端到端影子证据）。
>
> - 验证：`go build ./...`、`go vet`、`gofmt`、`go test -race ./internal/kernelscheduler/... ./internal/ares_evolution/... ./internal/ares_bootstrap/... ./internal/evolution/deployment/...`、以及 `make gate` 全部通过。
>
> - ✅ 已落地：**Step Y 度量半边（Y.2/Y.3 的 OBSERVE 阶段）+ P1-3 recover 修复（2026-09-04）**：
>
>   - 共享原语：新增 `internal/feedback`（纯数据包，零依赖标准库以外）承载 `Outcome`/`CollaborationOutcome`/`ToolCallOutcome`。生产方（`agentipc`、tool binder 装饰器）与消费方（`ares_evolution`）各自在消费点声明自己的 observer 接口，结构化满足 —— 内核 IPC 总线与工具层**零 import 进化层**。
>
>   - Y.2 协作度量：`agentipc.Bus` 新增 `WithCollaborationObserver`（`collaboration_observer.go`）。观测点在 `primitives.go` 的 **`Request`** **与** **`Send`** **两处出口**（`Delegate`/`Handoff` 走 `Request` 自动继承）。`feedback.CollaborationKind` 区分 `request`（回答）/`send`（投递受理）—— 两者度量的不是同一件事，混成一个成功率将不可审计。
>
>   - Y.3 工具度量：`sub.ObserveToolCalls` 装饰 `ToolBinder`（`tool_observer.go`）。选装饰器而非在执行体内加钩子，是因为生产有**两条执行体**（`sub` executor 与 `agentfabric.ChatCognition`），都从注入的 binder 取工具——包 binder 一处覆盖两条，且后续新增第三条执行路径无法绕过。
>
>   - 判决接入：`ChannelFeedbackRecorder`（`ares_evolution/channel_feedback.go`）把两通道写成 `collaboration` / `tool_call` **独立 source** 的 `KindFitness` 证据，按 `asm.Current()` 现取归因；`FitnessWeights` 新增 `Collaboration`/`ToolCall` 两维，`RuntimeFitnessAggregator.Window` 对两通道**按策略 scope**（不同于 workflow/scheduler/recovery 的 runtime-global）。
>
>   - 配置：`evolution.channel_feedback: {collab_enabled, collab_weight, tool_enabled, tool_weight}` 全默认关闭/0（`ares_config` + `configs/ares.yaml`）。`enabled` 管「是否记录」、`weight` 管「是否计入判决」，两者分离以便先审计证据再放权重。
>
>   - **P1-3 修复**（deep review 已核实项）：`Request` 的裸 goroutine 抽为 `Bus.invokeHandler` 并加 recover 边界，panic 转 `ErrHandlerPanic` 走与普通 handler error **相同的 sentinel-nil-reply 唤醒协议**（只 log 不唤醒会让调用方白等满 timeout）；panic 值不进 error（可能含内部路径/请求数据），只进注入的 logger（`Bus.WithLogger`，两个生产构造点已接）。`Send` 的同步 panic 刻意不 recover —— 它跑在调用方 goroutine 上，吞掉才是隐藏错误。
>
>   - 测试：`agentipc/collaboration_observer_test.go`（四种 outcome 表驱动、Send 生产路径、panic 不写假成功、panic 不烧 timeout、日志上下文键）、`ares_evolution/channel_feedback_test.go`（归因/未归因丢弃/未开通道零记录/source 隔离）、`ares_evolution/fitness_aggregator_channels_test.go`（weight=0 惰性、开权重后判决分化、跨策略不泄漏）、`cmd/ares/channel_feedback_wiring_test.go`（编译期接口断言 + 端到端 + 生产 Send 路径）。
>
>   - 验证：`go build`/`go vet`/`gofmt`/`golangci-lint`(0 issues)/`go test -race`/`make gate`/`git diff --check` 全过。
>
> - ⛔ **Step Y 作动半边未落地（下一步的真实工作量所在，见修订后的 Step Y）**：度量已通，但**进化拿到分数后没有对应旋钮可拧**——`mutation.Strategy` 的可变面只有 `PromptTemplate` + `Params`（LLM 参数），既无工具白名单/排序字段，agent 也没有「选择协作对象」的可调动作。原计划的 `ToolSelectionGenome` / `CollabGenome` 因此无法按原文交付。
>
> - ⭐ **Step Y 已立项（用户方向确认 2026-09-03，范围于 2026-09-04 修订）**：打通「单 agent 任务 + 跨 agent 协作 + 工具特征」三通道真实闭环（详见文末 Step Y）。

## 0. 未闭环边界（硬边界 — 0.3.1 vs 0.4.x 的划分依据，不可含糊）

> 本节是**发布措辞的唯一依据**。对外只能说清楚"哪些闭环了、哪些没有"；凡本节点名项，均不得在 0.3.1 发布文档/CHANGELOG 中以"闭环""完整"表述。

### N-1（P1-1，最关键）候选无自身证据 —— real-execution A/B ✅ 已落地（2026-09-03，见进度状态 Step 4）

- **原状**：G2 shadow 判定靠 `ReplayScorer` 读**策略自身历史**，不是"候选真跑一遍"；verdict 不是候选特异性的。

- **落地后**：`evolution.shadow_execution.enabled=true` 时，候选与活跃双臂在隔离 runner（工具调用接口级 deny、不接生产 EventStore）中对最近缓冲的真实任务副本各执行一次，配对结果直接进 G2 判决，双臂各留一条 `strategy_shadow` 来源、`shadow:true` 标记的证据。默认仍关闭；关闭时 replay 语义不变。

- **0.3.1 发布措辞**：可写"进化判决语义已闭环；开启 `shadow_execution` 后判决具备候选特异性（候选在隔离上下文真实执行）"；隔离执行的采样窗口与任务覆盖仍受 `sample_size` 与流量限制，不得写成"全量 A/B 验证"。

### N-2（缩水一）"真实 agent" = peer 拓扑，非单 agent 内 DAG

（Step Y.1 的改造对象：将供给**单 agent 内部工作流 DAG**，见 Step Y）

- **现状**：`buildLiveAgentDAG`（`cmd/ares/serve_live_dag.go:30`）建的是**一 peer 一节点**的顶层图（AgentType=主能力）。

- **边界**：`UpdateLiveDAG` 注入的是这个 peer 拓扑。若目标为"**单 agent 内部的工作流步骤图**"，那是另一个 gap，`UpdateLiveDAG` 喂进去的不覆盖它。

- **0.3.1 发布措辞**：写"进化作用于 peer 级 agent 拓扑"，不写"作用于单 agent 内部工作流"。

- **Step Y.1 状态**：已立项（2026-09-03），完成后 N-2 的"单 agent 内 DAG"部分即关闭。

### N-3（缩水二）测试覆盖率未达标的

- **现状**：总覆盖率 ≈**59.2%**，低于 GA 目标 **65%**。

- **薄弱区**：`shadow_sampler.go:93` 构造路径、`postgres/repositories`（0%，需真 PG）。

- **性质**：非发布硬阻断，但属于"质量就绪"欠账，应列入 0.4.x 补覆盖。

### N-4（收尾待办，非新缺口）非 serve 入口的显式日志 ✅ 已落地（2026-09-03，Step 7.3）

- `Bootstrap` 单独使用（非 serve）时保持合成图 + 关闭 deployment 的行为已在；显式日志已补（`bootstrap.go`，`cfg.Evolution.Enabled` 时输出 verdict-available-but-no-live-topology）。

- **性质**：行为已满足，仅是观测缺口，不改变闭环结论。

### N-11（Step Y 交付中）跨 agent 协作与工具特征不进入进化判定 —— 度量已闭环 ✅ / 作动：工具 ✅、协作 ⛔（2026-09-04 二次修订）

- **原状**：`agentipc.Bus` 无协作 success/timeout 度量，协作反馈完全未进进化；`agents/sub/executor.go` 的工具成败仅进经验蒸馏旁路（`emitSubTaskResult`），未作 evolved genome 特征。

- **原状描述的一处更正**：度量缺口在 `internal/agentipc/primitives.go`（376 行，Bus 的全部 primitive 在此），不在 `bus.go`（132 行，仅类型定义与注册）。行数无误但文件指错，接线时按 `primitives.go` 定位。

- **度量半边已落地（2026-09-04）**：`Request` 与 `Send` 两处出口各发一条协作回执（`feedback.CollaborationKind` 区分回答/投递受理）；每次工具调用经 binder 装饰器发一条回执。两者写 `collaboration` / `tool_call` 独立 source 的 fitness 证据，按 `asm.Current()` 现取归因，`RuntimeFitnessAggregator` 按策略 scope 加权读取。默认关闭。

- **作动半边：工具已落地 ✅ / 协作未落地 ⛔**——

  - 工具通道 ✅（2026-09-04，Y.3-ACT）：`Params["tools"]` 已接到两条执行体的 schema 过滤（`agents.ToolWhitelistFromParams` + `agentfabric/chat_cognition.go:311`、`sub/executor.go:867`；零交集回退全量并告警），且归一化后的工具串已并入 `ComputeEvidenceKey`（`mutation/types.go:199`，排序、顺序无关）。端到端断言 `tool_dimension_transmission_test.go`：仅 `Params["tools"]` 不同的两个策略在 `StrategyHash`、`EvidenceKey`、`tool_call` 通道、聚合 `Window` 四个环节全部分得开。原验收标准「『少调无用工具』的候选因工具成功率提升被 promote」**已可达成**（需 `tool_weight > 0`）。

  - 协作通道 ⛔：`agentsyscall` 对 agent 暴露的只有 `spawn_agent` / `create_task`，**没有「问某个 agent」这个可调动作**；协作由 bridge 按 topic 路由触发（`cmd/ares/evolution_ipc.go`），不是 agent 的决策。所以"问哪个 agent 该不该"目前不是策略能做的选择。

- **影响（再修订）**：工具维度已从「看得见改不动」转为闭环（默认 `tool_weight=0` 是刻意闸门，不是缺陷）；协作维度仍是**开环反馈**——证据在积累、判决分会分化，但下一代不会因此在协作维度上真的变好。

- **0.3.1 发布措辞（再修订）**：可写"协作与工具的真实成败已作为独立证据源进入进化判决（默认关闭）"，且可写"进化可作用于工具选择（白名单已接线，需开启 `tool_weight`）"；**禁止**写"进化作用于跨 agent 协作"——协作作动器未落地。

### N-12（2026-09-04 新增，同日两次修正）进化的可变面窄于三通道反馈的维度 —— 工具半边已闭 ✅

- **原判**：`mutation.Strategy` 顶层可变字段只有 `PromptTemplate` 与 `Params`，但工具旋钮**本就存在**——`Mutator.mutateTool`（`mutation/mutator.go:400`）读写 `Params["tools"]`（`string`，逗号分隔），由 `option.go` 的 `WithToolPool` 供候选。缺的是**接线 + 归因入 key**，不是缺字段。

- **已闭（Y.3-ACT）**：接线与归因两项均落地（见 N-11 工具半边）。同批修掉两个会让工具维度静默失效的缺陷：`Clone()` 不再继承 hash 缓存（否则子代命中父代 ScoreCache 拿到父代分数，选择压力归零）；`numericParam` 覆盖 int/uint/float 全族（此前裸 `float64` 断言丢掉 `top_k`/`max_steps`/`memory_limit`）。

- **仍缺的是协作半边**：agent 没有"问某个 agent"的字段/可调动作。工具与协作不可混为一谈。

- **遗留（2026-09-04 更新，不改变上述结论）**：原列三项已闭——`agentloop/engine.go` 第三执行体已接白名单（C5，`Request.ToolWhitelist`）、`Payload["tools"]→params` 装配优先级已定（C5，`agents.MergeNodeParams`，节点 Metadata > 全局策略）、工具集 allowlist 上界校验已接进选择路径（C6，`dream_cycle.findWinner` 调 `ValidateToolSet`，越界候选进 arena 前即被拒）。**真正仍缺的是** **`budget`/`prior`** **的消费语义**：两者只被装进 params，没有执行体实现"budget 用尽后该工具不再出现"（见 `Y1_SINGLE_AGENT_TOOL_DAG_DESIGN.md` §12.5-2）。

***

## 现状结论

已经闭合的：`generateDiffPatches`（`internal/ares_evolution/genome_wiring_system.go:975`）确实完成了 Snapshot → Mutate → Snapshot → Diff 的完整链路，不是空壳；`DeploymentPipeline` 已经在 `internal/ares_bootstrap/bootstrap.go:537-553` 接进 `Coordinator.SetDeployer`。

没闭合的是**判决环节**：变异能产出补丁，补丁能进部署管线，但管线用来判断「这个补丁好不好」的那个分数是假的，而且判决口径和配置语义不一致。所以现在的系统是「能变异、能晋升，但晋升与否和补丁质量无关」。

***

## Step 1 — 修复晋升判据的语义错位（P0，必须先做）

**问题**：`internal/evolution/deployment/deployment.go:215`

```go
if shadowScore < dp.config.PromotionThreshold {
```

`PromotionThreshold` 的文档定义是「the minimum fitness **improvement** required，Default: 0.05 (5% improvement)」——是**增量**。但这里拿 `shadowScore`（绝对分）去比。配合 `deployment_wiring.go` 的 `coldStartScore: 0.5`，结果是任何补丁都以 0.5 ≥ 0.05 无条件通过晋升。这是当前闭环里最致命的一处：门槛存在但恒真。

**做什么**：

1. `DeploymentRecord` 增加 `BaselineScore float64` 字段。
2. **改** **`StagingRuntime.Evaluate`** **签名为一次返回双值**，而非新增一个独立的 `Baseline(ctx)`：

   ```go
   // internal/evolution/deployment/deployment.go
   type StagingRuntime interface {
       Apply(ctx, p patch.RuntimePatch) (*patch.RuntimePatch, error)
       // Evaluate returns (shadowScore, baselineScore, err). shadowScore 是
       // 当前补丁作用后的分；baselineScore 是同一时间窗内未打补丁的当前
       // 活跃分。两者必须在同一次调用 / 同一时间锚点内采样——若拆成两次
       // 独立调用，并发写 evidence 时两窗可能不一致，delta 失真。
       Evaluate(ctx context.Context) (shadow, baseline float64, err error)
       Rollback(ctx, rollback *patch.RuntimePatch) error
   }
   ```

   结论：**不要新增** **`Baseline(ctx)`** **单独方法**——那会让 baseline 与 shadow 落在两个不同时间窗。把"取 baseline"并入 `Evaluate`，由 `deploymentStagingRuntime` 内部保证同一次 `Window` 采样口径一致。
3. `Deploy` 判据改成增量：

   ```go
   shadowScore, baselineScore, err := dp.staging.Evaluate(evalCtx)
   record.BaselineScore = baselineScore
   delta := shadowScore - baselineScore
   if delta < dp.config.PromotionThreshold { reject }
   ```
4. reject 的 `Reason` 同步改成打印 `delta` 和 baseline，便于审计追溯。

**验收标准**：

- 新增测试：baseline=0.60 / shadow=0.62（delta=0.02 < 0.05）→ `DeploymentRejected`；baseline=0.60 / shadow=0.70（delta=0.10）→ `DeploymentPromoted`。

- 新增测试锁定回归方向：shadow 绝对分很高（0.9）但 baseline 更高（0.95）→ 必须 reject。修复前这个 case 会 promote。

- **窗口一致性测试**：同一次 `Evaluate` 内 shadow 与 baseline 必须采样自同一时间锚点（用记录 `agg` 窗口查询次数 / 时间戳断言为一次查询、同一 since/until）。

- `go test ./internal/evolution/deployment/...` 全绿。

***

## Step 2 — 让 staging 评估变成 per-patch，而不是全局常量

**问题**：`internal/ares_bootstrap/deployment_wiring.go:64-73`

```go
res := r.agg.Window(ctx, "")
```

空 strategy 过滤 = 全局窗口。**每个补丁拿到的 shadowScore 完全相同**，与补丁内容无关。同时 `Apply`（:47）明确注释 "Preflight only ... do NOT touch any registry state"，`Rollback`（:75）只是 `applyCount--`。也就是说 staging 从没真正装载过补丁，评估的是打补丁之前的世界。Step 1 修好判据后，如果这里不修，baseline 和 shadow 会取到同一个全局均值，delta 恒为 0，所有补丁恒被拒——从「恒真」翻到「恒假」，同样不可用。这两步必须一起上。

**做什么**：

1. 证据侧已经具备条件：`internal/ares_evolution/observer.go:204` 有 `evidenceKeyStrategyID = "strategy_id"`，`writeEvidence`（:270）会写入。把补丁的 strategy/target 标识透传进 staging。
2. `deploymentStagingRuntime` 增加 `currentPatchStrategy string` / `activeStrategyID string` 字段，`Apply` 时记录 `p.Target`/strategy id。**`Evaluate`** **内部取双分**（对应 Step 1 改后的签名）：

   - shadow ← `r.agg.Window(ctx, r.currentPatchStrategy)`

   - baseline ← `r.agg.Window(ctx, r.activeStrategyID)`

   - 关键：**在同一次调用内、用同一 since/until 时间锚点**取这两窗，避免并发写 evidence 时两窗错位（Step 1 的窗口一致性约束）。可先落一个锚点再以该锚点构造两个 strategy 过滤查询。
3. `Rollback` 里清空 `currentPatchStrategy`，避免跨补丁串味。

**验收标准**：

- 单测：向证据库写入两个不同 strategy\_id 的证据（A=0.9，B=0.3），依次 Deploy 两个对应补丁，断言拿到的 shadowScore 分别为 0.9 和 0.3，而不是同一个均值。

- 单测：证据库为空时 shadow 与 baseline 都回落 `coldStartScore`，不 panic、不返回 0 导致误拒。

- **窗口一致性测试**：`Evaluate` 返回的 shadow 与 baseline 采样自同一时间锚点（断言两次 `agg` 查询的 since/until 相同）。

- 并发安全：`deploymentStagingRuntime` 现在有跨调用可变状态，`Deploy` 虽然全程持 `dp.mu`，仍需 `go test -race` 覆盖。

***

## Step 3 — 补上 live 回滚路径，激活 RollbackThreshold

**问题**：两处死配置，grep 确认全仓无读取方：

- `RollbackThreshold`（`deployment.go:41`）只在结构体定义和默认值里出现，没有任何执行路径。

- `ShadowSampleSize`（:33）同样零读取。

根因在 `deployment.go:236`：

```go
_ = liveRollback // retained for future live rollback
```

晋升后的回滚句柄被丢弃。而 `internal/evolution/patch/patch.go` 的 `Registry` 只有 `Register/Replace/Apply/ApplySet`，**没有 Revert**，所以回滚能力在底层就缺一块。

**做什么**：

1. `patch.Registry` 增加 `Snapshot(target)` + `Restore(target, snap)`：`Apply` 前抓取旧 executor/component，失败或回滚时还原。`ExecutorComponent.Snapshot` 目前返回 `ErrNoSnapshot`（patch.go:155），对这类组件退化为「保存旧 executor 引用并在 Restore 时 Replace 回去」。
2. `DeploymentPipeline` 保存 `liveRollback` 到 `DeploymentRecord`，并新增 `MonitorAndRollback(ctx, record)`：晋升后按 `EvaluationTimeout` 再采一次窗口分，若 `baseline - current > RollbackThreshold` 则调用 `live.Rollback` 并把状态改写为 `DeploymentRolledBack`。
3. `ShadowSampleSize` 二选一：接进 Step 4 的真实影子执行作为采样条数；若 Step 4 暂不做，则**从配置里删掉**，不留假旋钮。倾向删除——留着比删掉更有害。

**关键约束（修正）——live 目前没有真逆操作，Snapshot/Restore 是唯一回滚支点**：

- `deploymentLiveRuntime.Apply`（`deployment_wiring.go:94-99`）现在回传 **`&p`（同一指针）**，不是逆操作补丁。`deployment.go:236 "retained for future live rollback"` 丢弃的 `liveRollback` 因此**不是可执行的回滚句柄**。

- 所以 `MonitorAndRollback` 要真回滚，必须依赖 `patch.Registry` 的 `Restore` 还原 `Apply` 前的状态——**没有 Snapshot/Restore，"回滚到旧实例"就无从谈起**。结论：Step 3 的第 1 条（Snapshot/Restore）与第 2 条（回滚路径）**必须同批落地**，二者绑定，不能只做其一。

- `LiveRuntime` 接口也需同步加 `Rollback(ctx, snap)`（接收 `Snapshot` 产物）或让 `DeploymentPipeline` 直接操作 `patch.Registry.Restore`——取决于 live runtime 的封装边界，二选一并在实现里固定，测试按同一选择断言。

**验收标准**：

- 单测：晋升后注入一个回归到 0.4 的窗口（baseline 0.8，超出 0.10 阈值）→ 记录状态变为 `DeploymentRolledBack`，且 registry 里的 executor 已还原为旧实例（用指针相等断言）。

- 单测：回归幅度 0.05（未超阈值）→ 保持 `DeploymentPromoted`，不触发还原。

- `grep -rn "RollbackThreshold" --include="*.go" .` 必须出现在非 `_test.go` 的执行路径中（当前只有定义）。

***

## Step 4 — 影子评估接真实任务执行（工作量最大，可独立排期）

**问题**：`internal/ares_evolution/shadow_evaluator.go:439` `Evaluate` 是纯函数打分：

```go
activeScore := scorer(ctx, active)
shadowScore := scorer(ctx, shadow)
```

`scorer` 是注入的函数，不跑任务。全仓 grep `CopyTaskForShadow` / `ShadowExecutor` **零命中**——影子执行器不存在。所以「影子对比」目前是对策略对象打分，不是对策略产生的实际结果打分。

**做什么**：

1. 接线锚点已定位：`internal/kernelscheduler/scheduler.go:654` `executeWithCandidates`、:920 `buildQuantumStep`。新增 `ShadowExecutor`：复制 task 描述，用候选策略构建 quantum step，在隔离上下文中执行，产出的证据带 shadow 标记写入证据库（复用 `observer.writeEvidence`）。
2. 影子执行必须**只读副作用**：禁止写入生产 memory / 触发对外工具调用。这一条是硬约束，需要在 executor 层加显式开关，不能靠调用方自觉。
   **隔离边界（落地到可 review 粒度）**：`ShadowExecutor` 的执行上下文注入一个**副作用 deny-list ctx**，明确两类封禁——

   - 写生产 memory：任何经由 `agentfabric`/memory manager 的写路径一律短路，只允许读；

   - 触发外部工具：工具调用在影子 ctx 上被拦截（返回"shadow-mode: disabled"，不落盘、不发请求）。
     用**接口级 spy**（而非仅日志）在单测中断言整个影子执行期间对生产 memory store 的**写入调用次数 == 0**。
3. 配置：`configs/ares.yaml:105` 的 `evolution` 块下新增 `shadow_execution: {enabled: false, sample_size: N}`，**默认关闭**。全仓当前无 `shadow_execution` 键，属于新增。`sample_size` 若能落地即为 Step 3 的 `ShadowSampleSize` 提供真实取值前提。
4. `internal/ares_evolution/lifecycle.go:720` 的 `l.sampler.Prime(ctx, candidate, active)` 是现成的注入点，影子结果从这里回流。

**验收标准**：

- 端到端测试（挂在 `cmd/ares/e2e_grand_loop_real_test.go` 旁）：开启 shadow\_execution，跑一轮变异 → 断言证据库中出现带 shadow 标记的新证据，且 active/shadow 两侧 evidence count 均 > 0。

- 隔离性测试：影子执行期间对生产 memory store 的写入次数必须为 0（用 spy store 断言，且工具调用也被拦截）。

- 默认关闭验证：不改配置时行为与当前完全一致，`make gate` 无差异。

***

## Step 5 — 消除 G3 gate 的静默恒过

**问题**：`internal/ares_evolution/gate_eval.go:97`

```go
if g.registry == nil || g.runner == nil || len(g.suite.TestCases) == 0 {
    return true, 0, "eval suite not configured, skipping"
}
```

未配置即返回 `true`（通过）。`:135` 还有第二个同样的 pass-through（"no evaluators produced results, skipping"）。grep `g3.skipped` / `GateG3Skipped` **零命中**——跳过没有任何计数器或指标。生产环境里如果 registry 忘接，G3 会永久静默放行，且无从察觉。

**做什么**：

1. 增加 `skippedCount` 计数与结构化 warn 日志，跳过时输出具体缺失项（registry / runner / 空 suite 分别区分）。
2. 增加 `StrictMode bool`：开启时未配置直接返回 `false`（拒绝），而不是放行。生产配置置为 true。
3. 暴露跳过次数到可观测面，与现有 evolution 指标同路径。

**验收标准**：

- 单测：三种缺失场景各自产生一次 skip 计数且日志含对应原因。

- 单测：`StrictMode=true` + registry 为 nil → 返回 `false`，且 reason 明确说明是严格模式拒绝。

- 现有 `TestG2ConfigContract` 与 `make gate` 保持绿；`StrictMode` 默认 false 以免破坏现有 gate。

***

## 执行顺序与依赖

Step 1 和 Step 2 **必须同批上线**：单独做 Step 1 会把「恒真放行」翻成「恒假拒绝」，单独做 Step 2 则判据仍然错。这是一次原子修复。

Step 3 依赖 Step 1 的 `BaselineScore` 字段。Step 5 完全独立，可以随时插入。Step 4 工作量和风险都远高于前四步（涉及调度器和副作用隔离），建议单独排期，前四步先交付。

## 每步统一的验证动作

```
go build ./... && go vet ./... && gofmt -l .
go test -race ./internal/evolution/... ./internal/ares_evolution/... ./internal/ares_bootstrap/...
make gate
```

`make gate` 的实际内容是 `scripts/g1_reachability_gate.sh` + `TestG2ConfigContract` + `TestEventContract` + `-race -tags closure` 跑 `ares_evolution` 与 `ares_bootstrap` 两个包，所以 Step 1/2/3 的改动都会被它覆盖。

## Step 6 — 打通补丁的策略归因，解开 deployment 的自锁（P0）

Step 1/2 落地后暴露出一个原计划没写到的前置缺口：**没有任何补丁生产方会填 strategy 归因**。

- `RuntimePatch.Source` 装的是提议者类别（`diff.memory` / `candidate` / `ga`），`Target` 装的是组件名；证据库里的 `strategy_id` 来自 mutation 策略生命周期。这三个是互不相交的命名空间。

- 所以 per-patch 打分要用的那个 key 不存在。早期实现拿 `Source`/`Target` 兜底，结果是查询恒不命中、恒回落 cold-start，而返回值看起来仍像一次真实测量——这比不打分更危险。

- 现在的处理：`RuntimePatch` 新增 `StrategyID` 字段，`Apply` **只认** `StrategyID`，不做任何兜底；`Evaluate` 在任一侧 ID 为空时两侧同时返回 cold-start，delta 恒为 0，被正阈值拒绝；`deploymentAdapter.Deploy` 在入口就以显式错误拒绝无归因补丁，避免拒绝原因显示成 `delta 0.000`（读起来像"测量出来打平"）。

**当前后果**：`evolution.deployment.enabled=true` 时每个补丁都会被拒。这是有意的失败闭合——判决语义已正确，但判决所需的输入还没接上。deployment 默认 `Enabled: false`，所以默认行为不变，`make gate` 无差异。

### 6.1 生产侧：把策略 ID 透传进补丁

`generateDiffPatches(ctx, genomeReg, diffReg, nChildren)` 增加 `strategyID string` 形参，对返回的每个 patch 统一打标 `StrategyID`。**`strategyID == ""`** **直接返回 error**（fail fast），不允许不可归因的补丁流入 Coordinator。

两个调用点各自提供 ID，且必须与证据写入方用**同一个 key**：

| 调用点                                               | 传入的 ID                        | 谁往同一 key 写证据                                         |
| ------------------------------------------------- | ----------------------------- | ---------------------------------------------------- |
| `genome_wiring_system.go:915`（`RunIdleEvolution`） | `system.Population` 当代最优个体 ID | `recordOutcomesLocked`                               |
| `genome_wiring_run.go:416`（`submitToCoordinator`） | 该轮候选个体 `child.ID`             | `genome_wiring_run.go:367` 已写 `StrategyID: child.ID` |

第二个调用点是关键：`recordOutcomesLocked` 已经在用 `child.ID` 写 fitness 证据，所以补丁打上同一个 `child.ID` 后，`agg.Window(ctx, child.ID)` 立刻命中，shadow 侧不再恒回落 cold-start。这是打通归因**唯一必要**的一处对齐，不需要新造标识体系。

### 6.2 baseline 侧：`activeStrategyID` 必须每次 Evaluate 现取

现存缺陷（`bootstrap.go:547-553`）：`staging.activeStrategyID` 在 wiring 时从 `ASM.Current()` **取一次就冻结**。ASM 在运行期会晋升/回滚策略，于是 baseline 会指向一个早已不活跃的策略——delta 的分母漂了，而且不报错。

改法：`deploymentStagingRuntime` 持有 `asm *evolution.ActiveStrategyManager` 引用而非字符串快照，`Evaluate` 内调 `asm.Current()` 现取。`Current()` 已加读锁并返回 `Clone()`，并发安全。保留字符串字段仅供测试注入。

### 6.3 验收

- 候选补丁的 `StrategyID` == 证据写入的 `child.ID`，`Evaluate` 的 shadow 侧读到真实证据（非 0.5 cold-start）。

- ASM 晋升后再 `Evaluate`，baseline 跟着切到新的活跃策略。

- `generateDiffPatches` 传空 ID 返回 error。

- 端到端：`deployment.enabled=true` 时补丁**能被判决**（可晋升也可拒绝），不再一律被拒。这是本步的成功标志。

***

## Step 7 — 让进化真正作用于真实 agent（P0，闭环最后一环）

原计划把这条列为"不在计划内"，**该表述已过期**。核实后的实际状态：

**已经通了的部分**：`cmd/ares/serve_agents.go:66-81` 已调用 `buildLiveAgentDAG(cfg)` 把配置里的 agent 群体物化成真实拓扑（一 peer 一节点，`Capabilities[0]` 作 AgentType，沿用 legacy `Dependencies` 作边），注册到 `ares_runtime.AgentDAGLiveKey`，并 `UpdateLiveDAG(liveDAG)` 注入进化执行器——内部用 `graphExec.SetGraph` / `recoveryExec.SetDAG` **就地**替换占位 DAG（`Register` 无法覆盖已注册 key，这是 `provide_new_evolution_live_dag_test.go` 记录的已修缺陷）。memory 补丁本来就写 live `comp.Memory`。`serve_live_dag_test.go` 的 `TestUpdateLiveDAG_WiredFromServeShape` 已断言 recovery 补丁落在 live DAG 的 steps 上。所以"补丁只作用于占位物"对 serve 路径已不成立。

**真正剩下的洞（比占位更糟）**：`provide_new_evolution.go:341-343` 的注释自己写明 —— *"The genome registry's WorkflowGenome is NOT updated here because it needs a full re-registration"*。于是形成**跨图错配**：

- `WorkflowGenome`（`provide_new_evolution.go:119` 构造）仍包着 bootstrap 那个 3 步合成 DAG（`input→process→output`），**变异是在合成拓扑上算的**；

- 但补丁被应用到真实 agent DAG 上。

- 结果：patch 里的节点 ID 在目标图里根本不存在。这不是"作用于占位物"，是**在 A 图上推理、往 B 图上写**——要么静默 no-op，要么按不存在的节点报错，两种都会被误读成"补丁质量差"，而根因是拓扑源错了。

### 7.1 让 WorkflowGenome 跟上 live DAG

沿用执行器已有的就地更新模式：给 `WorkflowGenome` 加 `SetDAG(*engine.MutableDAG)`，由 `UpdateLiveDAG` 一并调用；若 genome registry 需要覆盖注册，则给它加 `Replace`。**不要**用"重新 Register"绕——已注册 key 无法覆盖，那是注定失败的 no-op，正是 7 前半段那个已修缺陷的同一个坑，别重犯。同时删掉那条"NOT updated here"的注释，它已不再描述实现。

### 7.2 补上"改动确实落到真身上"的断言

现有测试只断言注入链**被调用**，没断言**效果**。新增验收：

1. **同图性**：`UpdateLiveDAG` 后，`generateDiffPatches` 产出的 workflow patch 的节点 ID ⊆ live DAG 节点 ID 集合。这条直接钉死跨图错配。
2. **可观测变更**：一个 workflow 补丁走完 `Deploy`（`deployment.enabled=true`）后，`runtime.GetAgentDAG(AgentDAGLiveKey)` 的拓扑发生预期变化；`Rollback` 后恢复原拓扑。
3. **隔离不回退**：`closure_contract_test.go` 的 F04 断言（Bootstrap 时合成图只在 `evolution` key、不得占用 live key）保持通过——本步是给 live key 补供给，不是放宽隔离。

### 7.3 非 serve 入口的显式表态

Bootstrap 单独使用时没有 agent population，也就没有 live DAG。此时不要伪造一个：保持合成图 + 保持 deployment 关闭，并在日志里说明"判决可用，但无 live 拓扑可作用"。**沉默地在合成图上晋升，正是这份计划要消灭的那类问题。**

***

## 交付边界（2026-09-04 更新）

Step 1-5 交付「判决语义正确」；Step 6 交付「判决输入可归因」；Step 7 交付「判决作用于真实 agent 拓扑」；Step 4 交付「候选特异性判决（真实执行 A/B，默认关闭）」；**Step Y 的 OBSERVE 半边交付「三通道真实反馈进入判决（默认关闭）」，ACT 半边交付「工具选择可被进化作动」（Y.3-ACT，默认** **`tool_weight=0`）**。

**`进化已作用于真实 agent（单任务 + 协作）`** **目前仍不可写进发布文档。** 工具维度的 ACT 已闭环（Y.3-ACT），但协作维度仍是开环：判决分会因协作真实成败而分化，下一代在协作维度上的**行为**却不会因此改变（N-11 二次修订、N-12）。

仍需如实声明的边界：

| 编号   | 内容                                | 状态                                                |
| ---- | --------------------------------- | ------------------------------------------------- |
| N-3  | 覆盖率 59.2% < 65% GA 目标             | 0.4.x 补覆盖；`postgres/repositories` 需真 PG 环境，非补测试可解 |
| N-11 | 工具：看得见也改得动 ✅；协作：**看得见、改不动**（开环反馈） | OBSERVE ✅ / ACT：工具 ✅、协作 ⛔                         |
| N-12 | 工具半边已闭（接线 + 归因入 key 均落地）；协作仍无可调动作 | 工具 ✅；协作需新字段/syscall                               |
| N-2  | 进化改不到单 agent 内部步骤                 | Y.1 已重估为改执行模型，拆出单独立项                              |

### 下一步排期建议（按「能否独立完成」排序）

1. **~~Y.3-ACT（工具通道接线 + 归因入 key）~~** ✅ 已完成（2026-09-04）。原列三项小尾巴（第三执行体接线、`Payload["tools"]→params` 优先级、工具集 allowlist 上界）已随方案 C5/C6 全部闭合；剩余边界见 `Y1_SINGLE_AGENT_TOOL_DAG_DESIGN.md` §12.5。
2. **~~P1-2~~**~~（`agentloop/engine.go`~~ ~~吞错无注释）~~ ✅ 已落地（2026-09-04，与 C1 同批复核）：`engine.go:346-356` 等处的最佳失效注释已存在并核实为完整（`_ = AddStructuredMessage`/`_ = AddMessage` 均带 "best-effort" + 理由）。
3. **Y.2-ACT（协作 syscall）** — `ask_agent` syscall 已随 commit b501eb10 落地；与「影子 deny-list 扩到协作」绑定同批。
4. **Y.1（单 agent DAG）** — 方案 C（§11 C1–C7）执行记录见 `Y1_SINGLE_AGENT_TOOL_DAG_DESIGN.md` §12（2026-09-04）。**诚实状态**：C1/C4/C5-tools 生产真实生效；C6 接线进 dream\_cycle 选择路径；C7 为真实集成测试（事件→投影→evidence→WindowToolStep）；C2 投影层可运行但**未接进 serve 循环**（部署决策）；C3 生产 tool\_call evidence 经 recorder 写入、`WindowToolStep` 可读出。**budget/prior 消费语义未落地**。`make check` 全绿。详见 §12.5 边界。

***

## Step Y — Y 方案：打通「单 agent 任务 + 跨 agent 协作」的真实闭环（用户确认方向：2026-09-03；范围修订：2026-09-04）

> **第一性原理（用户）**：agent 不可能主动感知外部世界，它只能通过三条通道被动感知——**单 agent 任务、工具调用、跨 agent 协作回执**。进化的输入必须是这三条通道真实观察到的反馈，不能是外部臆测。
> **目标**：让进化环 ①干活→②度量→③判定→④改→⑤部署→回到① 覆盖**单 agent 任务 DAG + 跨 agent 协作**，而不只是 peer 拓扑。判定标准仍是一条：**下一代的 agent，因上一代三条通道的反馈而变得更好。**

> **2026-09-04 范围修订（重要）**：原 Y.2/Y.3 各把「度量」与「作动」写成一件事，实际是两件，且难度不对称。度量已全部落地；作动的前置条件（策略的可变面）原计划未识别，见 N-12。
>
> 每条通道现在拆成两个可独立验收的半边：
>
> | 半边              | 含义                         | 判定标准                        |
> | --------------- | -------------------------- | --------------------------- |
> | **OBSERVE**（度量） | 通道的真实成败变成归因到策略的 fitness 证据 | 证据库里查得到、按策略 scope、独立 source |
> | **ACT**（作动）     | 判决分能改变下一代在该通道上的**行为**      | 存在一个策略字段，改它能改变该通道的行为        |
>
> 只有 OBSERVE 就是**开环**：分数在动，行为不动。这是修订的核心动因——原计划的验收标准（如「少调无用工具的候选被 promote」）跨在两个半边上，无法用来验收任何单一交付。

### Y.0 现状核实（2026-09-03 首次核实 / 2026-09-04 更新）

| 通道         | OBSERVE                                      | ACT                                                                                                            | 代码位置                                                                                |
| ---------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| 单 agent 任务 | ✅ 已闭环（`RuntimeObserver` 读任务事件）               | ⚠️ 只作动到 peer 拓扑，改不到 agent 内部步骤（N-2）                                                                            | `agentloop.Engine.Run` / `executor.Execute`                                         |
| 工具调用       | ✅ **已落地**（2026-09-04，binder 装饰器）             | ✅ **已落地**（2026-09-04，Y.3-ACT）：`Params["tools"]` 已接两条执行体的 schema 过滤 + 并入 `EvidenceKey`，默认 `tool_weight=0` 为刻意闸门 | `sub/tool_observer.go` / `agentfabric/chat_cognition.go:311` / `agents/strategy.go` |
| 跨 agent 协作 | ✅ **已落地**（2026-09-04，`Request` + `Send` 双出口） | ❌ **无可调动作**：agent 没有「问某个 agent」的工具，协作由 bridge 路由触发                                                             | `agentipc/primitives.go`（非 `bus.go`）/ `cmd/ares/evolution_ipc.go`                   |

**结论（二次修订）**：`ShadowExecutor`（N-1/Step4）已把"候选实跑"做实；三条通道的 OBSERVE 全部闭环；ACT 半边工具维度已闭环（接线 + 归因均落地）。**剩下的 ACT 工作量集中在协作维度**——它缺的不是接线而是「问某个 agent」这个可调动作本身（N-12）。

### Y.1 单 agent 内部任务 DAG 供给（闭 N-2 的一半）—— ⛔ 已重估：改执行模型，非接线，单独排期

**问题**：`buildLiveAgentDAG`（`serve_live_dag.go:30`）建的是 peer 拓扑；单 agent 内部的任务流转 DAG **没有供给** `UpdateLiveDAG`。进化改了 peer 图，却改不到"单个 agent 怎么干活"。

**2026-09-04 重估（原计划的三步接线不成立）**：原文假设「`agentloop` 里 agent 实际跑的那套」已经是一个 `engine.MutableDAG`，只是没注册。核实后不是：

- 生产执行体是 `agentfabric.chatCognition`（`chat_cognition.go`），跑的是 **ReAct 消息循环**——`chatStepState{Round, Messages[]}` 一轮一轮推进，状态是消息数组 + 轮次计数器，**不存在节点与边**。

- `agentloop.Engine.Run` 同理（`engine.go` 的 `st.toolCount` / `resp.ToolCalls` 循环）。

- 所以 Y.1 的第 1 步「把它构建好的 `engine.MutableDAG` 暴露出来」没有对象可暴露——**得先把消息循环重新表达成 DAG**。

**这意味着什么**：Y.1 不是给已有结构补注册，是**改单 agent 的执行模型**（ReAct 循环 → 可变图）。它触碰 quantum/checkpoint 契约（`chatStepState` 是 resume 的 PCB，schema 有版本校验，§6 持久化规范）、`ExecuteStep` 的 yield 语义、以及 scheduler 的量子调度。工作量与风险比 Y.2/Y.3 的 ACT 半边高一个量级。

**结论**：按 code\_rules §0.4（方向优先于接线），**Y.1 从 Step Y 中拆出，单独立项、单独经方向认可**。它与 Y.2/Y.3 绑在同一个 Step 里会让整个 Step Y 无法交付。N-2 的"单 agent 内 DAG"部分在此之前保持未闭环，发布措辞维持"进化作用于 peer 级 agent 拓扑"。

**若将来立项，前置问题（先答再动手）**：ReAct 的每一轮是否天然是一个节点？工具调用是节点还是边上的动作？一个"图"上的 mutation（插入/删除节点）在消息循环语义下对应什么行为改变？——这三问没有答案之前，做出来的 DAG 会是个没人能解释其 mutation 含义的装饰品。

### Y.2 跨 agent 协作反馈进进化

#### Y.2-OBSERVE ✅ 已落地（2026-09-04）

**原计划的两处偏差**（核实后更正）：

1. **观测点写错了文件**：原文写 `agentipc.Bus`（`bus.go`，132 行）。Bus 的 primitive 全在 `primitives.go`（376 行），`bus.go` 只有类型定义与 Register。
2. **只观测** **`Request`** **会在生产环境记录零条证据**——这是最关键的一处。全仓非测试代码**没有任何地方调用** **`Request`**：`Delegate`/`Handoff` 是它的包装，其余只有 `examples/`。生产 peer 通道走的是 `ipc.Send` → `Bus.Send`（`cmd/ares/evolution_ipc.go:132`），fire-and-forget。按原计划实现，所有单测会通过，而真实部署的 `collaboration` 源恒为空。

**实际做法**：

1. 观测点在 `primitives.go` 的 **`Request`** **与** **`Send`** **两处出口**（`Delegate`/`Handoff` 走 `Request` 自动继承）。`feedback.CollaborationKind` 区分 `request`（回答）/ `send`（投递受理）——fire-and-forget 没有"答案"可判，但"我寻址的 agent 不存在"（`not_found`）与"它的 handler 拒收"（`failure`）是关于发起方选错协作对象的真实反馈。两者不合并成一个成功率：度量的不是同一件事，混起来不可审计。
2. `ChannelFeedbackRecorder` 写 `source="collaboration"` 的 `KindFitness` 证据，payload 含 `strategy_id`（`asm.Current()` 现取，不冻结快照——同 Step 6.2 的理由）、`kind`、`outcome`、`latency_ms`（仅审计，不折进分数：协作回执没有标定过的延迟预算，现编一个会让 fitness 不可审计，同聚合器 cost/latency 惩罚项的搁置理由）。
3. **`Broadcast`** **刻意不观测**：topic 扇出没有单一 target 可归因，把聚合投递数记成一条协作分，等于让发起方为它没选择过的订阅者背责。
4. `OutcomeUnobserved` 语义：调用方自己 cancel（context 取消）不产生记录——发起方走开了，这不说明被问的 agent 好坏。`outcome` 初值即 `Unobserved`，只在已知出口显式赋值，未预见的返回路径**不写记录而非写假成功**。

**验收结果**：`collaboration_observer_test.go` 四种 outcome 表驱动 + `Send` 生产路径 + `Delegate`/`Handoff` 继承；`cmd/ares/channel_feedback_wiring_test.go` 端到端穿 `EvolutionAwareIPC.Send`（bridge 的真实调用形态）断言证据落库。

#### Y.2-ACT ⛔ 未落地：**没有可调动作**（原计划的 `CollabGenome` 不成立）

原文第 3 步要把"问题该问哪个 agent / 协作该不该发起"做成可进化策略。核实后：**agent 没有这个动作可选**。

- `agentsyscall` 对 agent 暴露的工具只有 `spawn_agent` 与 `create_task`（`syscall.go:19,22`），没有"问某个 agent"。

- 协作由 bridge 按 topic 路由触发（`evolution_ipc.go:105` 的 `topicDelegateTask`/`topicPipelineStage`/`topicOrchestrateWrk`），**是路由行为，不是 agent 的决策**。

- 所以协作成功率能测到，但进化拿到分数后无处施力——除了改 prompt 文字暗示它别去问某人。

**前置条件（属方向性改动，需先经认可）**：给 agent 增加一个"请求协作"的 syscall（如 `ask_agent(target, topic, payload)`），使"问谁"成为 agent 的可观测决策，进而成为策略可约束的对象。**这是新增 syscall 语义，按 §0.4 先经方向认可再动手。** 在此之前 Y.2-ACT 不排期。

#### Y.2 影子隔离（原 Y.5 第 3 条）⛔ 未落地

`ShadowExecutor` 的 deny-list 目前只拦工具（`shadowDenyBinder`），**不拦协作**。影子执行中的候选若触发协作，会打到生产 bus 上。当前风险为零——影子 cognition 用的是 `shadowDenyBinder` 且不接生产 EventStore，没有协作发起路径——但一旦 Y.2-ACT 给 agent 加了协作 syscall，这条**必须同批落地**，否则影子会产生真实副作用。**记为 Y.2-ACT 的绑定前置。**

### Y.3 工具调用反馈作进化特征

#### Y.3-OBSERVE ✅ 已落地（2026-09-04）

**原计划的一处偏差**：原文写"在 `executor.go:924 executeToolCall` 的 `CallTool` 结果处"埋点。生产有**两条执行体**——`sub` executor 的工具循环（`executor.go:924`）与 `agentfabric.chatCognition`（`chat_cognition.go:380`），后者才是 peer 模式的生产路径（A1.4 把工具循环下沉进了 agentfabric）。只改 `executor.go` 会漏掉生产主路径。

**实际做法**：`sub.ObserveToolCalls` 装饰 `ToolBinder`（`tool_observer.go`）。两条执行体都从注入的 binder 取工具，包 binder **一处覆盖两条**，且后续新增第三条执行路径无法绕过。装饰器嵌入接口而非手写转发：将来接口新增方法时不会静默丢实现。

`outcome` 分类：`nil` → success；`ErrToolNotFound` → **not\_found**（策略要了一个不存在的工具，是决策错误，与"工具跑了但失败"不同，值得区分）；其余 → failure；ctx 已死 → `Unobserved`。装饰位置在 planner bridge 挂载**之后**，所以 planner 兜底解析的调用也被计量。

#### Y.3-ACT ✅ 已落地（2026-09-04）

原文要"新增 `ToolSelectionGenome`"。核实后**不需要新增任何字段**——工具旋钮已存在（`mutation.Strategy.Params["tools"]`，`string` 逗号分隔，`Mutator.mutateTool` 会变异它），缺的是**接线到执行器 + 归因入 EvidenceKey**，两项现已完成。

**实际做法**：

1. **接线**：新增 `agents.ToolWhitelistFromParams`（`agents/strategy.go`）解析 `Params["tools"]`，两条执行体在把 schema 转成 LLM 工具**之前**过滤（`agentfabric/chat_cognition.go:311`、`sub/executor.go:867`）。过滤在投给 LLM 之前而不是在 `CallTool` 处拒绝：让 LLM 看见再拦会浪费一轮，并把"被策略自己禁掉"误记成 `not_found`。
2. **归因入 key**：`ComputeEvidenceKey`（`mutation/types.go:199`）并入归一化工具串（trim + 排序，故 `"b,a"` 与 `"a, b"` 同 key）。这是与"只接执行器"的本质差别——只接执行器改变行为但不改变归因，两个只在工具上不同的策略仍会落在同一 key、`tool_call` 证据混在一起。
3. **空白名单 guard**：白名单与注册工具零交集时**回退全量**并告警，不把空工具表交给 LLM（否则 `available_tools=0` 饿死）。空/缺失 `Params["tools"]` = 不过滤，默认行为不变（零值可用，§5.4）。
4. **同批修掉两个静默失效点**：`Clone()` 不继承 hash 缓存（否则子代命中父代 ScoreCache、拿父代分数，选择压力归零）；`numericParam` 覆盖 int/uint/float 全族（裸 `float64` 断言会丢 `top_k`/`max_steps`/`memory_limit`）。另把 `extractToolNames` 的词表从裸动词换成真实注册名别名表（`experience_hints.go`），否则 guided 变异持续写出零交集白名单。

**验收结果**：

- `TestToolWhitelistFromParams` / `TestToolWhitelistZeroIntersectionFallsBackToFullSet`：旋钮能拧，且零交集不致空表。

- `TestComputeEvidenceKey_IncludesToolsField` / `_ToolOrderIndependent` / `_NoToolsField` / `_IncludesIntegerParams`：归因分得开、顺序无关、无工具字段时不加后缀。

- **端到端** `tool_dimension_transmission_test.go`（§9.5 最小验收信号）：按四链断言"仅 `Params["tools"]` 不同 → fitness 不同"——①`StrategyHash` 分得开 ②`EvidenceKey` 分得开 ③`Weights.ToolCall>0` 通道已武装 ④`Window` 真读到差异，失败工具集的均分更低。配套 `TestToolDimension_UnarmedChannelIsInert` 把默认 0 权重固化为**刻意闸门**：同样的证据在未武装时不移动任何分数。

**遗留（2026-09-04 更新，不影响本项验收）**：初稿列的三项已随方案 C 闭合——第三执行体（`agentloop/engine.go`）已接过滤、`Payload["tools"]→params` 优先级已定、工具集 allowlist 上界已接进 `findWinner`。剩余边界见 `Y1_SINGLE_AGENT_TOOL_DAG_DESIGN.md` §12.5（`budget`/`prior` 无消费语义；`Projector` 未接 serve 循环；genome 候选生成路径未接护栏）。

### Y.4 三条通道合并成"一条线"（收口）✅ 机制已就绪（做法与原计划不同）

**原计划**：把 `strategy_shadow` 证据扩展为含任务/工具/协作三项子维度，G2 一次判定读全维度。

**实际做法（更干净，且不破坏 N-1 隔离契约）**：三通道走**独立 source**，由 `RuntimeFitnessAggregator` 加权合并，而不是塞进同一条证据：

- `FitnessWeights` 新增 `Collaboration` / `ToolCall` 两维，`Window` 对这两源**按策略 scope**（不同于 workflow/scheduler/recovery 的 runtime-global——协作回执与工具成败是策略"选择去问/去调"的结果，归给别的策略就是错误归因）。

- 两维默认权重 0，且 opt-in 源在 weight≤0 时**整源跳过**而非贡献 0 权重：被计入的源同时推高 `totalCount`，而那是 staging 路径的判决门槛——没人开启的通道不得用样本数去许可一次判决。

- 扩展 `strategy_shadow` 的原方案会把三种不同度量标准塞进同一条记录，反而破坏 N-1 刚建立的"影子记录不污染活跃分"隔离。

**验收结果**：`fitness_aggregator_channels_test.go` 断言 weight=0 时惰性（分数与样本数均不变）、开权重后两个任务通道表现相同的策略因协作/工具差异而判决分化、跨策略证据不泄漏。

**仍未闭环的部分**：这是 OBSERVE 侧的收口。三维**判决**已可验证，三维**作动**取决于 Y.2-ACT / Y.3-ACT。

### Y.5 一致性前提（不破坏已落地实现）

- ✅ 新增通道全部走**独立 evidence source**（`collaboration` / `tool_call`），不污染 `strategy` 与 `strategy_shadow`（守住 N-1 隔离契约，`channel_feedback_test.go` 有专门的 source 隔离断言）。

- ✅ 全部默认关闭（实际键名 **`evolution.channel_feedback.{collab_enabled, collab_weight, tool_enabled, tool_weight}`**，非原文的 `collab_feedback` / `tool_features`）。`enabled` 管"是否记录"、`weight` 管"是否计入判决"，两者分离：可先让证据进审计轨、确认合理后再放权重。默认行为与 `make gate` 无差异（已验证）。

- ⛔ `ShadowExecutor` 的 deny-list 扩到协作——见 Y.2 影子隔离，当前无协作发起路径故风险为零，但与 Y.2-ACT 绑定同批。

### Y 验收（端到端，修订）

原验收「开启全部开关跑一轮完整进代，候选因某项真实提升被 promote，部署后下一轮表现提升」**跨在 OBSERVE 与 ACT 两个半边上**，在 ACT 缺失时无法用于验收任何单一交付。拆为两级：

**Y-OBSERVE 验收 ✅ 已达成**：开启 `channel_feedback` 两通道 + 非零权重，跑真实协作与工具调用 → 三通道各产生归因到策略的独立源证据 → G2 判定读三维 → 两个任务通道表现相同的策略因协作/工具差异而判决分化。

**Y-ACT 验收：工具维度 ✅ 已达成 / 协作维度 ⛔ 未达成**：候选**因自己在某通道上的不同选择**（而非同构下的运气）被 promote。工具维度由 Y.3-ACT 落地，`tool_dimension_transmission_test.go` 断言仅工具白名单不同即产生不同判决分（需 `tool_weight > 0`）；协作维度需先有协作 syscall。

**发布措辞的硬约束（二次修订）**：可以写「进化已作用于工具选择（默认关闭）」；**不可以**写「进化已作用于真实 agent（单任务 + 协作）」——协作 ACT 未达成，单 agent 内部步骤仍受 N-2 限制。

***

## Step Y 附带 code review（对已落地实现的针对性复核）

> 针对 Step 4/N-1 已落地的 ShadowExecutor 实现做了复核，结论：质量高、意图正确，无 P0 bug。

| 检查点                                                                        | 结果                                                                                    |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `shadow_executor.go` `shadowEvidenceSource="strategy_shadow"` 独立 source    | ✅ 正确——不污染 `strategy`，守住 rollback/staging 不被影子记录污染（注释已说明）                              |
| `shadowTaskBuffer=32` 最近任务缓冲                                               | ✅ 合理，证据来自近期流量                                                                         |
| `shadowExecTimeout=15s` 外层超时                                               | ✅ 防止 Prime 内联阻塞进化心跳                                                                   |
| deny binder（`shadowDenyBinder`）接口级 `ListTools`空+`CallTool`拒绝、生产 binder 零接触 | ✅ 隔离硬边界，spy 断言                                                                        |
| `ShadowSampler.SetExecutionFeeder` 无产出回退 replay                            | ✅ 语义不变，安全降级                                                                           |
| 配置默认关闭                                                                     | ✅ 不改变现有行为，`make gate` 无差异                                                             |
| 唯一保留                                                                       | ⚠️ 候选实跑只在 `evolution.shadow_execution.enabled=true` 才触发；默认关闭时仍是 replay（N-1 已如实标注，非缺陷） |

**新增问题**（Y 范围内）：`agentipc.Bus` 无协作度量（Y.2）与 `executor` 工具特征未进进化（Y.3）——两者是 Y 的交付对象，非已落地代码的缺陷。

***

## Step Y OBSERVE 半边 code review（2026-09-04，对本批落地实现的自检）

> 对 Y.2-OBSERVE / Y.3-OBSERVE / P1-3 三项落地实现的复核。

| 检查点                                                            | 结果                                                       |
| -------------------------------------------------------------- | -------------------------------------------------------- |
| `internal/feedback` 纯数据包（零依赖标准库以外），生产方与消费方各自在消费点声明 observer 接口 | ✅ 内核 IPC 总线与工具层零 import 进化层；接口在消费方（§5.2）                 |
| `Send` 与 `Request` 双出口观测                                       | ✅ 生产路径（bridge 走 `Send`）确实被覆盖——这是原计划会漏掉的那处                |
| `CollaborationKind` 区分 request/send                            | ✅ 两者度量对象不同，不合并成单一成功率                                     |
| `outcome` 初值 `Unobserved`，只在已知出口赋值                             | ✅ 未预见的返回路径（含 panic unwind）不写假成功                          |
| 调用方 cancel → 不产生记录                                             | ✅ 发起方走开不说明被问方好坏；`Observable()` 在 recorder 侧统一过滤          |
| `Broadcast` 不观测                                                | ✅ 扇出无单一归因对象，记聚合投递数是错误归责                                  |
| 工具侧用 binder 装饰器而非执行体内埋点                                        | ✅ 一处覆盖两条生产执行体，第三条路径无法绕过；嵌入接口，将来新增方法不会静默丢实现               |
| `ErrToolNotFound` 单列 `not_found`                               | ✅ "策略要了不存在的工具"是决策错误，与"工具跑了但失败"分开                         |
| 装饰位置在 planner bridge 之后                                        | ✅ planner 兜底解析的调用也被计量                                    |
| 独立 source + 按策略 scope + weight=0 整源跳过                          | ✅ 守住 N-1 隔离；未开启的通道不得用样本数许可 staging 判决                    |
| `enabled` / `weight` 分离                                        | ✅ 可先审计证据再放权重（分阶段采纳）                                      |
| 归因用 `asm.Current()` 现取                                         | ✅ 同 Step 6.2 的理由，不冻结快照                                   |
| 不可归因记录丢弃并计数（`Dropped()`）                                       | ✅ 静默丢弃比计数丢弃更坏；`latency_ms` 仅审计不折进分数                      |
| P1-3：panic 走与 handler error 相同的唤醒协议                            | ✅ 只 log 不唤醒会让调用方白等满 timeout（有专门测试断言不烧 timeout）           |
| P1-3：panic 值不进 error，只进注入 logger                               | ✅ 可能含内部路径/请求数据（§3.5）；库层不直接打印（§9.1），两个生产构造点已接 logger      |
| P1-3：`Send` 的同步 panic 刻意不 recover                              | ✅ 跑在调用方 goroutine 上，调用方可自行 recover，吞掉才是隐藏错误              |
| 队列有界（256）+ 满则丢弃并计数                                             | ✅ 丢一条 fitness 样本可接受，给 agent 的工具调用加上存储延迟不可接受              |
| 遗留                                                             | ⚠️ 影子 deny-list 未扩到协作（当前无协作发起路径，风险为零；与 Y.2-ACT 绑定）       |
| 遗留                                                             | ⚠️ 协作维度 OBSERVE 已闭环但 ACT 未闭环 → 开环反馈，见 N-11 二次修订（工具维度已闭环） |

**验证**：`go build ./...`、`go vet ./...`、`gofmt -l .`、`golangci-lint run`（0 issues）、`go test -race`（agentipc / ares\_evolution / agents/sub / ares\_bootstrap / ares\_config / aresrecovery / introspect / cmd/ares）、`make gate`、`git diff --check` 全部通过。

***

# 附录 M — 计划收敛合并（2026-09-05）

> 为收敛根目录计划文档，本附录把以下三个文件的关键信息（去重后）并入本主计划，随后删除原文件：
> `AGENT_OS_END_TO_END_CLOSURE_PLAN.md`、`ARES_FULL_DEEP_REVIEW_2026-09-03.md`、`DAG_FIRST_EXECUTION_PLAN.md`。
> 本附录只做**信息保全**，不重写已落地结论；凡与本主计划正文重复处不再赘述，以免二次膨胀。

## M.1 端到端闭环深化（原 END\_TO\_END，2026-09-03 / HEAD c5f36d45，draft v1）

> 目标：「进化从能跑，变有意义」——三条互相咬合的验收主张：每条业务链路端到端 / 候选有自身证据 / 进化有意义。

### M.1.1 两条进化系统（事实，不合并）

| <br /> | v1 `internal/ares_evolution`                                    | v2 `internal/evolution`                                                   |
| ------ | --------------------------------------------------------------- | ------------------------------------------------------------------------- |
| 进化对象   | 策略 `mutation.Strategy`（temperature/top\_k/prompt）               | 工作流 DAG / knowledge / memory（`RuntimePatch`）                              |
| 门控链    | G1 guardrail → G2 shadow → G3 eval → G4 staging（`lifecycle.go`） | CandidateVerifier 三选网（G1结构/G2evidence/G3regression）+ Coordinator+Deployer |
| 证据来源   | `evidence.Store` KindFitness（记策略自身历史）                           | Coordinator 评估回调                                                          |
| 候选特异   | **否**（v1，回退 prior）                                              | 部分（v2）                                                                    |

两套都在线上跑、都不能删 → 收敛是 0.4.x 单独工作；本计划不合并它们。

### M.1.2 六支柱业务链路（端到端清单，可被 system\_runtime/orchestrator 编排）

1. **任务/调度**：`taskfabric` + `kernelscheduler`（executeWithCandidates → RunQuantum → attribution）
2. **进化归因**：`aresrecovery` DeterministicScorer / ExecutionAttribution（RecordWithMetrics 吃 latency/retry/recover）
3. **证据存储**：`evidence.Store`（Memory/Postgres）+ `RuntimeObserver.writeEvidence`（observer.go:285 记 `source="strategy"`）
4. **门控判定**：`StrategyLifecycle` G1–G4（lifecycle.go）+ ShadowEvaluator/ReplayScorer
5. **DAG/工作流编排**：`system_runtime` + `taskfabric.PlanLoop`（GAP-2 / K1–K5）
6. **预报回滚**：`RollbackPolicy` + bootstrap recovery loop

### M.1.3 验收矩阵（E1–E5 与 Step 对应）

| #  | 陈形                                          | 现况                         | 对应                           |
| -- | ------------------------------------------- | -------------------------- | ---------------------------- |
| E1 | 候选 G2 判定前确实执行过且有自己的 evidence                | 否（replay 只读历史）             | Step A / 本计划 N-1（✅ 已落地）      |
| E2 | G2「更好」由候选自身结果产生                             | 否                          | Step A / N-1（✅）              |
| E3 | v2 patch 从真实 live 工件产生、经 staging、promote/回滚 | 部分                         | Step B / 本计划 Step 2/3/6/7（✅） |
| E4 | 六支柱每跳有测试与可观测                                | 缺「归因→门控→回流」接缝              | Step C                       |
| E5 | G3/G4 零-token 与满配置行为明确、不橡皮图章                | G3 缺 registry 有意缺失；G4 默认未验 | Step D                       |

### M.1.4 明确不做（防过度设计）

- 不重写 `mutation.Strategy`/`Coordinator`/kernel 架构；不删旧 GA（v1 保留 baseline 对照）；不引入新外部依赖/进化库。

- `shadow_execution` / v2 patch 回流默认关闭，需配置显式打开。

- 收敛前置（0.4.x）：抽 `internal/evolution/verify/` 共享 G1–G4 门控 + evidence 读写 + promote/rollback，v1/v2 复用。

## M.2 全仓深度 review（原 ARES\_FULL\_DEEP\_REVIEW，2026-09-03，基准 code\_rules\_v2）

> 36 万行 Go 单体，代码质量整体高，但有三类系统性问题。方法：AST 规模扫描 + 4 路并行 subagent + 逐项人工核实；「已核实」=开源确认，「需确认」=模式扫描待复核。**不编造不存在的 bug。**

### M.2.1 真 bug / 未闭环（P0/P1）

- **P1-1 候选无自身证据（G2 非候选特异）** ✅ 已闭环（2026-09-03 Step 4 / N-1）。

- **P1-1b 协作与工具通道「看得见改不动」** ✅ 度量已闭环（Y.2/Y.3-OBSERVE）；作动工具维度已闭环（Y.3-ACT），协作维度仍开环（N-11/N-12）。

- **P1-2 error 被吞无注释** ✅ 已闭环（2026-09-04，engine.go 已补 best-effort 注释）。

- **P1-3 裸 goroutine 无 recover** ✅ agentipc 部分已修（Bus.invokeHandler，2026-09-04）；`Send` 同步 panic 刻意不 recover（见正文复核表）。

- **P1-4 超长生产函数隐藏风险**（非漏洞但高风险）：`mutable_dag.go:659 ReplaceNode 154行`、`write_buffer.go:285 flushBatch 127行`、`validator.go:79 validateValue 115行`。

- **P1-5 异步嵌入死信无人强制回收** ⚠️ 需确认：`embedding_dead_letter` 存量清理/重试未见回收循环，死信累积不回收 → 写入可能卡 NULL vector（Query 过滤 `embedding IS NOT NULL`）。**W1 待办**。

### M.2.2 测试敷衍（P3）

- **T-1** `*_coverage_test.go` 巨型硬凑（4 个）：workflow/engine/coverage\_test.go(608行)、postgres/coverage\_test.go、manager\_coverage\_test.go、offline\_coverage\_test.go——多为「调方法断言不 panic」，少反向/错误路径 → 虚增覆盖率。

- **T-2** 测试用 `time.Sleep` 同步 **251 处**（规范 §7.3 明令禁止）：runtime\_test.go 45、shutdown\_comprehensive\_test.go 19、scheduler\_test.go 13、runtime\_resurrection\_test.go 11。风险 flaky/CI慢。

- **T-3** 空壳/无断言测试 ⚠️ 需逐文件确认。

### M.2.3 规范违规（P2，已量化）

| 项                              | 结论                                                                                                                                      |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| V-1 单文件 >1000 行                | ✅ 6 个：lifecycle.go 1287 / scheduler.go 1185 / dream\_cycle.go 1081 / genome\_wiring\_system.go 1059 / fabric.go 1048 / executor.go 1044 |
| V-2 单函数 >100 行                 | ✅ 156 个（多为表驱动/多分支，功能正确但违上限）                                                                                                             |
| V-3 库层 `fmt.Println/log.Print` | ✅ 65 处（ares\_protocol、若干 load/init）                                                                                                     |
| V-4 注释代码块                      | ✅ 142 处（事件/工具/工作流模块）                                                                                                                    |
| V-6 硬编码魔数（goconst）             | ✅ 47 处抽样（`30 * time.Second` 等）                                                                                                          |
| V-7 吞错                         | ✅ introspect/control.go 43 处 HTTP encode best-effort（可接受需注释）；knowledge 21 处需确认                                                          |

### M.2.4 整改建议优先级（2026-09-03 快照）

- W1：`embedding_dead_letter` 死信重投/过期清理（P1-5）；拆 6 个 1000+ 行文件 + 156 个 100+ 行函数（V-1/V-2）；灭 251 处 test Sleep（T-2）；清 142 注释代码 + 65 库层打印 + 47 魔数（V-3/V-4/V-6）。

- W2：重构 4 个 `*_coverage_test.go` 为真语义断言（T-1）；`agentfabric` 未覆盖模块补深查。

- 已闭环标注：P1-1/P1-1b(度量)/P1-2/P1-3、补扫 N-8（agentsyscall/plan.go:246 goroutine 已确收受管）。

（注：本附录 M.2 为某次 review 快照；其中「待办」若与本主计划正文冲突，以正文最新落地状态为准。）

## M.3 DAG-First 执行模型（原 DAG\_FIRST\_EXECUTION\_PLAN，方向草案）

> 状态：**方向草案，需按 code\_rules\_v2 §0.4 经确认后再动手**。目标：单 agent 内按 `MutableDAG` 逐步推进，撤销顶层 ReAct 循环；ReAct 只保留为单个节点类型，不再作为执行模型。约束：不引入新依赖、不并存两套执行循环（§5.1）、零值可用（§5.4）、单文件<1000 行 / 单函数<100 行。

**状态注记（2026-09-05，仅追加，不改原文）**：M.3 方向草案已升格为独立主线文档 `TOOL_DAG_MAINLINE_DESIGN.md`（两层同构 MutableDAG、M0–M6 落地序列、四缺口附录 R），本 M.3 保留为草案历史。后续实施以主线文档为唯一依据。

### M.3.1 为什么

- 现在：`chat_cognition.go` 的 `chatStepState{Messages[]}` 线性推进，进化只能拧 `Params["tools"]`，拓扑靠事后投影（`toolprojection`）硬造。

- 已有：`workflow/engine` 的 `MutableDAG` + patch/differ/executor + per-node Metadata，进化已能直接 patch 它（Y1-C4）。

- 结论：让任务**真正按图跑**，投影层退役。LLM 自主性不丢——退到 Planner（产图）与节点内的单次决策。

### M.3.2 v1 范围（只做这些）

- `DAGCognition`：实现现有 `agentfabric.Cognition` 接口，一节点一量子，拓扑序顺序执行。

- 节点类型只两种：`tool`（调一次 `ToolBinder.CallTool`）与 `llm`（一次文本总结，可选）。

- Planner 只确定性模板：按 `task.AgentType` 查静态步骤；`task.Payload["dag"]` 显式给图时优先。

- checkpoint v2（§M.3.4），事件沿用 C1 契约。

**明确不做（v1）**：LLM Planner（prompt 产图，v2 事）、并行分支（v1 串行）、动态扩图（执行中 InsertNode，v1 禁止）、`budget/prior` 消费（v1 只透传 tools）、重试/恢复策略（v1 fail-fast）、新包/新存储/新事件类型。

### M.3.3 Planner 接口

```go
type Planner interface {
    Plan(task *models.Task) ([]*engine.Step, error) // 返回 nil,nil = 无模板，调用方回退
}
```

- 实现只一个：`StaticPlanner map[AgentType][]StepTemplate`；`StepTemplate{ID, Tool, ArgsJSON}`。

- ID 去重/空 ID/未知工具名 → Plan 直接 error，不产半张图；`task.Payload["dag"]` 覆盖模板。

- 位置：`internal/agentfabric/dag_plan.go`。

### M.3.4 checkpoint v2（dagStepState，schema==2）

```go
type dagStepState struct {
    SchemaVersion int               `json:"schema_version"` // == 2
    TaskID        string            `json:"task_id"`
    Steps         []*engine.Step    `json:"steps"`
    Done          []string          `json:"done"`
    Outputs       map[string]string `json:"outputs"`
    Params        map[string]any    `json:"params,omitempty"`
}
```

- decode：`SchemaVersion != 2` 拒绝；`TaskID` 不一致拒绝恢复（同 v1）。

- 输出截断 4KB，超限截断 + 记 `truncated:true`；无 hash 字段（同 v1 强度）。

### M.3.5 量子语义（一节点一量子）

1. 无 checkpoint → Plann.Plan 建冻结图；空图/Plan error 即 error（fail-loud，§0.2）。
2. 拓扑序取第一个不在 Done 的节点（`DAG.GetExecutionOrder` 直接用）。
3. `tool` 节点：`ToolBinder.CallTool` → 写 Outputs → 发 C1 两事件（round=节点序号，seq=0）→ 追加 Done → 有剩余即 yield，无剩余 Done=true。
4. `llm` 节点：`LLMAdapter.GenerateWithParams`（无 adapter 则跳过，输出 ""，不失败）。
5. 任一节点 error → 整任务 error（v1 无重试）。

Result 组装：每节点一条 `RecommendItem`（ItemID=nodeID，Content=Outputs\[id]，超 500 字截断），不做 LLM 汇总。`ctx` 首参透传；无新增 goroutine；`ToolBinder` 仍走现有白名单过滤（Y.3-ACT）。

### M.3.6 回滚预案

- 总闸：`agentfabric.DAGExecution.Enabled=false` 默认关闭（零值=老行为，§5.4）；关闭时 CognitionFactory 返回老 chatCognition，开启时返回 dagCognition。

- checkpoint 互斥：老代码读 v2 即拒绝；新代码读 v1 即拒绝并建议重提任务，不做 v1→v2 迁移（任务短生命周期）。

- 删除路径（合入前须 §5.5+§8.4）：验证通过后删 `chat_cognition.go` 循环体（保留 ToolBinder/ChatClient 接口定义供复用），grep `chatStepState/decodeChatStepState` 引用归零，`toolprojection` 标记 Deprecated 下一版删除。

- 任一步骤失败：关闸即回老路，无数据残留。

### M.3.7 文件与验收

- 新增（≤2）：`dag_cognition.go`（dagStepState+dagCognition）、`dag_plan.go`（Planner+StaticPlanner）。

- 复用：`workflow/engine/{types,mutable_dag}`、`agents/strategy.go` 白名单、`ares_events` C1 契约。

- 测试（合同优先，禁 Sleep，§7）：

  1. `TestDAG_OneNodeOneQuantum`：3 节点恰 3 次 ExecuteStep（前 2 yield 第 3 Done），顺序=拓扑序。
  2. `TestDAG_ResumeRefusesForeignTask` + `TestDAG_RejectsV1Checkpoint`：身份与版本校验。
  3. `TestDAG_ToolFailureFailsTask`：中间节点失败即 error，无静默跳过。
  4. `TestDAG_EmptyPlanFailsLoud`：无模板+无 Payload\["dag"] 即 error。
  5. 存量 `make check` 全绿。

- 关系：Y1-C2 投影层退役；Y1-C4/C5（Metadata 作动/装配）从「补丁 hack」变「原生语义」；N-2（单 agent 内 DAG）即其落地。

