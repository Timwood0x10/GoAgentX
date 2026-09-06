# ARES AgentOS 项目评审与缺陷清单

> 评审对象：分支 `dev`，2026-09。所有结论均基于代码实测（`grep`/`read` 定位到文件行号），非泛泛而谈。
> 用途：作为**修补路线图**，每条缺陷给出「位置 / 严重度 / 影响 / 修复建议」。

---

## 〇、一句话总评

这是一个**架构自洽、概念原创**的 "Agent 即操作系统进程" 运行时内核。核心创新——把 LLM 对话历史当作**可序列化、可迁移、可恢复的进程状态**——在工程上被 `checkpoint + lease + epoch fencing` 三件套坐实了。但它目前是一个**研究级内核**，离生产就绪还差若干处**承重墙级别的缺口**（内存回收、IPC 可靠性、DAG 终态合成、成本反馈闭环）。下面逐条列出。

---

## 一、值得肯定的工程决策（先给应得的分数）

修补前先确认哪些**不要动**——这些是对的：

| 机制 | 位置 | 为什么对 |
|---|---|---|
| 中毒循环防护（ReAct 轮次上限） | `chat_cognition.go:162-164`（`defaultMaxToolRounds`）、`shadow_compare.go:81` | 防止 LLM 无限调用工具烧钱 |
| 重启退避 + 次数上限 | `aresrecovery/recovery.go:61-67`（5 次、1s→30s 指数退避） | 防止崩溃-重启风暴 |
| 任务级重试预算 | `taskfabric/task.go:51-52`、`fabric.go:396` | 失败任务不会无限回炉 READY |
| Epoch fencing | `fabric.go:267 Acquire` / `ownerLocked` | 过期持有者无法驱动已易主任务，杜绝双写 |
| 错误包装一致性 | 全仓 `%w` 801 处 vs `%v/%s` 298 处 | 约 73% 正确包装，可栈追溯 |
| 并发原语密度 | `sync.Mutex/RWMutex` 206 处、`atomic` 36 处 | 共享状态普遍上锁，非裸奔 |
| 测试投入 | 646 个 `*_test.go` / 1503 个 `.go`（≈43%） | 关键路径有测试兜底 |
| caller provenance 不可伪造 | `chat_cognition.go:473-476`（`kernelctx.WithCallerID`） | syscall 不信任 LLM 参数，安全内建 |

**结论**：作者懂分布式系统、懂 Go、懂安全。下面的缺陷不是"没想到"，多数是**已标注 `TODO(tech-debt)` 但尚未落地**——这恰恰是最该优先补的，因为它们是承重墙。

---

## 二、缺陷清单（按严重度排序，可直接开工）

### ✅ P0-1　终态任务内存泄漏：Reaper 未接线（2026-09-06 已修复，遗留见 P0-1a/b）

**位置**：`internal/taskfabric/reaper.go:28`
```go
// TODO(tech-debt): no production caller wires this reaper yet, so terminal L2
// session tasks still accumulate.
```
**影响**：`Reaper` 结构体、`Sweep()` 全实现了，但**没有任何生产代码调用它**。长驻 `serve`/`peer` 进程里，每个 L2 会话的终态任务（COMPLETED/FAILED）永远留在内存 task map 中，**单调增长直到 OOM**。这是最典型的"长跑必炸"缺陷。

**落地（2026-09-06）**：三条修复建议全部执行——
1. `NewReaperWithKeep` 加 keep 谓词（nil = 旧墙钟语义）；`cmd/ares/peer_mode.go` `createPeerAgents` 内经 `runBackground` 托管循环接线（serve 模式同走此函数，两模式覆盖），`gracePeriod` 走 `kernel.dag_execution.reaper_grace` 配置（缺省 30s 单一真相源在 taskfabric）。
2. keep-set 以 `SessionRegistry` 为唯一权威：`sessionKeepSet`（`cmd/ares/session_admission.go`）经 `SessionIDFromNode` 反解任务 ID → 会话存活即永不收割（decision C 语义：墙钟 grace 不再能吃掉长会话的可读历史）。
3. 回归测试：`TestReaper_KeepSetProtectsLiveSession`（grace=1ns 下存活会话跨边界不误删、释放后可收割）、`TestSessionKeepSet`、`TestSessionIDFromNode`（ID 反解往返）、`TestResolveReaperGrace`、`TestValidateKernelDAGExecution/negative_reaper_grace`。

**修复后复查发现两条残留（新账，见下）**：keep-set 把"fabric 单调增长"转化成了"注册表条目永不死则任务永不收割"——泄漏路径没消失，只是换了持有者。

---

### 🔴 P0-1a　会话永不释放 → keep-set 永久钉住任务（P0-1 残留）

**位置**：`internal/agentfabric/session_registry.go`（无 TTL/过期机制）+ `cmd/ares/session_admission.go:47`
**事实链**：
- 释放点只有两处：answer 体执行成功（`l2graph.go:429`）与准入失败回滚（`session_admission.go:80`）。
- 会话准入后若**永远到不了 answer**——planner 量子持续失败、工具错误循环、多轮会话被客户端放弃、answer 量子在执行到 `ReleaseSession` 前失败——注册表条目永久存活。
- keep-set 对存活会话**无条件保留**（这正是 P0-1 的语义），所以这些会话的全部终态任务**永不收割**：泄漏从 fabric 单调增长变成"每个被放弃的会话钉住一份"，量级变小但没有归零。
- 更糟：编译订阅挂在 `context.WithoutCancel(ctx)` 上（`session_admission.go:47`），`session_registry.go:106` 注释承诺的"未释放会话随 owner ctx 消亡"兜底**在准入路径上不成立**——WithoutCancel 之后只有进程退出才取消。

**修复方向**：注册表条目加 `lastAccess`（`GetSession` 触碰刷新）+ `SweepExpired(idle)`，挂进同一个 reaper 后台循环；idle 窗口只需覆盖最长合法会话（depth × 量子时长），15–30min 量级安全。answer 失败路径可另行补 release。

---

### 🟠 P0-1b　含 `/` 的 session_id 击穿 keep-set（P0-1 残留）

**位置**：`cmd/ares/peer_mode.go:636/651`（`session_id` 取自客户端 payload，无格式校验）+ `agentfabric.SessionIDFromNode`（按首个 `/` 反解）
**事实链**：客户端提交 `session_id="a/b"` → 节点 ID `sess/a/b/d1/grep#0` → `SessionIDFromNode` 反解出 `"a"` → 注册表里只有 `"a/b"` → keep=false → **存活会话的历史任务在 grace 后被收割**，planner 组装上下文读到已删 envelope。这正是 decision C 禁止的事故类别，且触发条件是纯客户端输入。
**修复方向**：准入处校验 session_id 不含 `/`（fail-fast 拒绝，与空 ID 同级）；确定性 ID 构造器（`SessionRootID`/`SessionNodeID`）的无斜杠前提应成为显式契约。

---

### 🔴 P0-1c　answer 后同 ID 重提交静默继承上一会话状态（多轮主流程即触发）

**位置**：`cmd/ares/session_admission.go:66-71`（root `ErrTaskExists` → adopt）+ `internal/planprojection/coordinator.go:411-422`（节点 `compileOrAdopt`：`ErrTaskExists` 永不浮出）+ `internal/agentfabric/l2graph.go:350-355`（root 以会话 prompt 为输出完成）
**事实链**（正常多轮流程，非对抗输入）：
1. 轮次 1 走完 answer → `ReleaseSession`；其任务留在 fabric 等收割（grace 30s + sweep 1min，最坏 ~2min 窗口）。
2. 轮次 2 带**同一 `session_id`** 到达（客户端续聊的自然行为）：注册表已空 → `InitSession` 成功 → root `CompileNode` 撞 `ErrTaskExists` → 被当作"部分失败重试"**adopt**。
3. 但被 adopt 的 root 是轮次 1 的 **COMPLETED** 任务，envelope 里是**轮次 1 的 prompt**（`rootCognition` 把 input 写进输出）。planner `readNodeOutput` 读到非空 → payload 回退（只在空时触发）永不生效 → **轮次 2 的 LLM 上下文里会话 prompt 是轮次 1 的**。
4. 更深的污染在节点层：轮次 2 长出 `sess/s1/d1/grep#0`，与轮次 1 同名（确定性 ID = 同深度同工具同实例）→ `compileOrAdopt` 撞 `ErrTaskExists` → adopt + refresh，但**终态任务不会被调度器重跑** → 轮次 2 把**轮次 1 的工具输出当成自己的新结果**读进上下文。全程零报错。
5. keep-set 此时反向作动：新会话"存活"，把这些陈旧任务**永久保护**起来，reaper 再也清不掉。
6. 测试缺口：B2-1 的 `ResubmitReusesSession` 只测**活会话**重提交（多轮续写）；**释放后**同 ID 重提交（新会话复用 ID）无用例。

**修复方向**：准入处 root 撞 `ErrTaskExists` 时查存量任务状态——非终态 → adopt（现行为，部分失败重试正确）；**终态 → 定向收割 `sess/<sid>/` 前缀全部任务后重编译**（收割安全性由"旧会话已释放、无 planner 在读"保证，只是把 reaper 的活提前做）。节点层撞名在 root 收割后自然消失。补"释放后同 ID 重提交拿到全新 root、prompt 不串、同名节点真实重执行"回归测试。

**附带发现（P2 级）**：准入竞态小窗——`InitSession` 竞态败者提前返回 nil，胜者若随后 root 编译失败 → release → 败者已提交的任务 stranded（planner `ErrSessionNotFound` 永久失败，落入 P0-1a 泄漏路径）。窗口小，修 P0-1a 的 idle TTL 可兜住。

---

### 🔴 P0-2　IPC 无重试 / 死信"有仓库、没入库"

**位置**：`cmd/ares/kernel_loop.go:3-5`
```go
// TODO(tech-debt): agentipc has no retry/dead-letter semantics (the legacy ahp
// DLQProcessor was removed with the leader-sub protocol). Wire IPC retry or a
// dead-letter path when multi-agent messaging scales (repair plan GAP-3).
```
**复核修正（2026-09）**：`agentipc` **已有** `DeadLetterStore`（`bus.go` `DeadLetters()` 暴露、有界 FIFO，capacity 0→1024，见 `deadletter.go`），**但 `.Record()` 在 `bus.go` 内零调用点**——投递失败从未写进死信，是"有仓库、没入库"。因此 `无重试` 这一半仍成立；`无死信` 这一半应理解为"未接线"而非"不存在"。且 `Send` 对 `ErrAgentNotRegistered`/`ErrNoHandler` 会向调用方返回错误，**并非全静默**——真正的静默丢失发生在目标已注册但忙（订阅 channel 64 缓冲满）时，发布被 `default:` 丢弃。

**影响**：`ask_agent` / `Send` / `Delegate` / `Handoff` 这类跨 agent 消息，目标 agent 忙/缓冲打满时消息**静默丢失**（调用方无感）。多 agent 协作的可靠性契约因此不成立——`ask_agent` 拿到"已发送"回执，但对方可能永远收不到。

**修复建议**：
1. 给 `agentipc` 加**有界重试**（指数退避，复用 `recovery.go` 已有的退避常量风格）。
2. **把失败投递与 Handler 异常接到已存在的 `DeadLetterStore.Record`**（这正是 `Bus.DeadLetters()` 存在的意义，改动远小于"新建 ipc_dlq"）；如需持久化再桥接 `ares_events`，带 `EventIpcDeadLettered`，供 `aresrecovery` 或人工 triage。
3. `ask_agent` 语义上区分"已入队"与"已投递"，LLM 侧只暴露"已入队"，避免它误以为对方已收到。

---

### 🔴 P0-3　DAG 终态 answer 节点无合成器（主线功能不完整）

**位置**：`internal/agentfabric/l2graph.go:393-404`
```go
// answerCognition terminates the session on its terminal node. It does NOT
// summarize ... TODO(tech-debt): no summarizer is wired here. Synthesizing an
// answer needs the PREDECESSORS' outputs ... reachable only by widening the
// Cognition interface, which the mainline invariant forbids.
```
**影响**：这是**最伤主线叙事**的缺口。planner 生长 L2 图 → 子任务执行 → 汇聚到 answer 节点，但 answer 节点**只会原样吐出自身携带的 content，没有 content 就打一条 warn 日志**（`l2graph.go:420-424`）。也就是说 "规划→执行→**合成最终答案**" 这条 DAG 主线，**最后一公里是断的**。当前只有走 `chatCognition` 的 legacy ReAct 路径能真正产出综合答案。

**根因很硬核**：合成需要读**前驱任务的 envelope**，而 `Cognition` 接口的唯一输入是"自己的任务"—— widening 接口又违反主线不变量。这是设计张力，不是简单 bug。

**修复建议**（作者已暗示方向）：
- 不要 widening `Cognition`，而是新增一条**专用 answer 路径**：沿图路径把前驱输出组装进 prompt，再调 LLM。即让 answer 节点成为一个"会读图"的特殊执行体，由调度器在依赖满足后注入组装好的上下文，而非让 Cognition 自己去够前驱。
- 过渡期：在 `shadow_compare.go` 里把"DAG 臂无法合成答案"记为已知 mismatch 类别，避免误判为回归。

---

### 🟠 P1-4　两条 ReAct 实现并存（维护翻倍）

**位置**：`internal/agentloop/engine.go`（28KB，SDK 路径）与 `internal/agentfabric/chat_cognition.go`（Kernel 路径）
**实测**：两者**仅在注释里互相引用**（`engine.go:205`、`engine.go:579` 提到 "peer executors (chat_cognition.go)"），代码零复用。工具白名单/预算/事件发射语义是**各写一遍**。

**影响**：任何行为调整（如新增一层工具可见性闸门、改预算扣减时机）要改两处并保证语义不漂移，长期必然分叉。

**缓解现状（给分）**：作者**已经意识到并在动手收敛**——新增的 `internal/agentfabric/shadow_compare.go` 是一个**双路径影子对比**工具：同一 prompt 分别喂 `chatCognition`（legacy）和 `plannerCognition`（DAG），用"只 advertise、全 deny"的 binder 保证零副作用，比对两条臂的工具调用序列是否一致，并把 mismatch 归档 triage。这正是迁移验证的正确姿势。

**修复建议**：
1. 把 ReAct 的**纯决策内核**（给定 messages+toolSchemas → 产出 tool_calls / 终答）抽成**单一共享函数**，两条路径各自只保留"如何持久化状态"的差异（SDK 内存、Kernel checkpoint）。
2. 用 `shadow_compare` 的归档结果作为"可安全切换默认路径"的量化门槛（如连续 N 天 mismatch 率 < x% 再切）。

---

### 🟠 P1-5　进化适应度不含成本/延迟（经验闭环跑偏）

**位置**：`internal/ares_evolution/fitness_aggregator.go:292-294`
```go
// TODO(tech-debt): subtract the cost/latency penalty term here once a
// real cost/latency data source reaches the EventStore.
```
**影响**：`ConfidenceSource` 会把历史成功率作为先验注入调度打分（`fabric.go:534-542`），但适应度目前**只算成功率，不减成本/延迟惩罚**。后果：调度器会把同类任务**持续派给"能成功但很贵/很慢"的 agent**，与"资源配额 + 预算"的治理目标直接冲突。经验闭环优化的是错误的目标函数。

**修复建议**：
1. 先打通**成本/延迟数据源到 `ares_events`**（token 用量、墙钟延迟已在 `endQuantumOutcome` 的 latency 参数附近，`scheduler.go:1037`，需确认落库）。
2. 在 `fitness_aggregator.go:290` 的 `mean` 之后减去 `λ_cost·norm(cost) + λ_lat·norm(latency)`，λ 走配置。
3. 冷启动（`weightSum==0`）已有 `ColdStartScore` 兜底，保持。

---

### 🟠 P1-6　Postgres 迁移无法回滚

**位置**：`internal/storage/postgres/migrate.go:203-214`
```go
func RollbackLast(...) error {
    return errors.Wrap(errors.ErrRollbackUnsupported, "rollback last migration")
}
// TODO: introduce a schema_migrations version table to enable real rollback
// (expected by 2026-12-31).
```
**影响**：迁移是"一坨幂等 DDL、无版本表"，`RollbackLast` 直接返回不支持。生产升级一旦某次 DDL 有破坏性变更，**没有回滚路径**，只能靠备份恢复。对"操作系统级"定位的项目是硬伤。

**修复建议**：引入 `schema_migrations(version, applied_at)` 表，迁移拆成带 `Up()/Down()` 的有序条目；`RollbackLast` 按版本表倒放。作者已排期 2026-12-31，建议提前，因为它是**越晚越难补**（历史迁移没有 Down 就是永久债）。

---

### 🟡 P2-7　checkpoint 无大小上限 / 消息历史不裁剪

**位置**：`chat_cognition.go` 的 `chatStepState{Messages, Round, ...}` 全量序列化进 `task.Payload["checkpoint"]`；全仓**未搜到** checkpoint 尺寸上限或对话窗口裁剪/摘要机制（`grep maxCheckpoint|truncat|sliding|compress.*message` 在 `taskfabric`/`agentfabric` 无命中）。
**影响**：长任务（几十轮 ReAct）的 `Messages` 单调增长，每量子 yield 都**全量重写**进 payload → ① 存储写放大（Postgres/SQLite 高频大 blob）；② 反序列化后直接喂 LLM，迟早**撑爆上下文窗口**。

**修复建议**：
1. 给 checkpoint 里的 `Messages` 加**滑动窗口 + 头部摘要**（保留 system + 最近 K 轮 + 中段摘要）。
2. 或引入**增量 checkpoint**：只存"相对上一量子的 diff"，配合事件溯源重建全量。
3. 至少加一个 `MaxCheckpointBytes` 护栏，超限降级为"截断 + 记 warn"（`planner_cognition.go:301` 已有 "truncated past, surface it and keep walking" 的先例可借鉴）。

---

### 🟡 P2-8　请求路径上的 `context.Background()`（取消传播断裂）

**位置**：`internal/` 非测试代码 120 处 `context.Background()`。多数合法（后台循环、detached 清理，如 `ares_ctxutil/ctxutil.go` 的 `WithDetachedLabel` 是**故意的**脱离父 ctx）。但混进了**请求作用域**的写路径，例如：
- `taskfabric/fabric.go:837`：事件 `Append` 用 `context.Background()`——一次已被取消的请求，其事件写入无法被取消/超时约束。
- `ares_skills/catalog.go:134/340`：`FetchHTTPManifest(context.Background(), ...)`——外部 HTTP 拉取无请求级超时（虽然 `:319` 另有 2min 超时包裹，但另两处裸奔）。

**影响**：取消/超时不能端到端传播，慢依赖（DB、外部 HTTP）可拖住本应被取消的操作，goroutine 滞留。

**修复建议**：审计这 120 处，把**请求作用域**的 `context.Background()` 换成透传下来的 `ctx`；确需脱离父取消的（清理、落盘）统一走 `ares_ctxutil.WithDetachedLabel` 并**加独立超时**，让"故意 detached"和"忘了传 ctx"在代码里可区分。

---

### 🟡 P2-9　被吞掉的 error（量级 ~200–650 处，视口径）

**位置**：全仓非测试 `_, _ :=` / `, _ :=` / `_ =`。**口径提示（2026-09 复核）**：不同形态计法差异大——errcheck 近似扫描非测试代码约得 226 处，而并列的 `_ =` 宽松计数可到 652 处。因此"652"应视作**量级信号**而非精确审计数，落地以 `errcheck`/`golangci-lint` 实测为准。
**影响**：其中大量是合法的 `defer func(){ _ = Close() }()`，但比例偏高，热路径里可能有**被静默丢弃的真实失败**（如某次 `Append`/`Send` 返回 error 被 `_` 吃掉）。分布式系统里"吞 error"= 丢可观测性 = 事故时无线索。

**修复建议**：跑一遍 `errcheck`/`golangci-lint`（项目有 `Makefile`，确认 lint 目标是否已含 errcheck），对**非 defer-Close** 的忽略逐个加 `_ =` 显式注释理由，或改为记 warn 日志。

---

### 🟢 P3-10　协作式抢占：量子内 hang 死无硬杀手段

**位置**：`fabric.go:570 Preempt`（注释明确"不做 OS 式硬抢占"）。
**影响**：一个量子 = 一次 LLM 调用 + 工具执行。若 LLM/工具 hang 住，内核**无法强制杀死**，只能等 ctx 超时。失控 agent 会占用一个并发槽（`WithMaxConcurrent`，默认并发≤32）拖慢 drain 节拍。
**修复建议**：这是 Go 协作式模型的固有约束，**不必强改**。但应确保 `RunQuantum` 外层始终包 `context.WithTimeout`（per-quantum deadline），把"hang"转化为"超时→yield/失败→按重试策略回炉"，让协作式抢占在**有界时间**内一定生效。属加固而非重构。

---

### 🟢 P3-11　调度器无"排空后停止"的优雅关闭

**位置**：`Scheduler` 只有 `Run(ctx)`（`scheduler.go:307`）与 `Running()`，**无 `Stop/Shutdown`**（方法列表实测确认）。停止靠 ctx 取消。
**影响**：ctx 取消时，进行中的量子可能被中途放弃。因有 checkpoint 机制，任务会在别处续跑，**数据不丢**，故严重度低；但进程退出瞬间的 lease 未主动释放，恢复要等 TTL 过期才接管，**重启窗口内有调度空窗**。
**修复建议**：加一个 `Shutdown(ctx)`：停止取新任务 → 等在途量子到安全点 yield（带超时）→ 主动释放 lease。把"等 TTL"变成"主动交接"，缩短重启抖动。

---

## 三、修补优先级建议（Roadmap）

```
P0（阻塞生产 / 破坏主线叙事）
  ├─ P0-1 Reaper 接线 ✅（2026-09-06，keep-set + 配置 grace + 回归测试）
  │     ├─ P0-1c answer 后同 ID 重提交串会话状态（多轮主流程即触发）← 三条残留中最重，先修
  │     ├─ P0-1a 会话永不释放钉住任务（注册表 idle TTL）   ← P0-1 残留，开闸前必修
  │     └─ P0-1b session_id 含 "/" 击穿 keep-set（准入校验）← 一行校验，随 P0-1c 同批改
  ├─ P0-3 DAG answer 合成器（主线最后一公里）       ← 决定"DAG 主线"能否宣称完成
  └─ P0-2 IPC 重试 + 死信（多 agent 可靠性契约）
        └─ 合并 A-3：给 agentipc.Message 加 TraceID 并贯穿 ctx（一次 IPC 改造，性价比最高）

P1（目标函数 / 运维安全）
  ├─ P1-5 适应度减成本/延迟（先打通 cost→EventStore）
  ├─ P1-6 迁移版本表 + 真回滚（越晚越贵，建议提前于 12-31）
  └─ P1-4 抽共享 ReAct 决策内核（用 shadow_compare 量化切换门槛）

P2（规模化前的护栏）
  ├─ P2-7 checkpoint 尺寸上限 + 消息窗口
  ├─ P2-8 请求路径 ctx 透传审计
  └─ P2-9 errcheck 清零非 defer 吞错

P3（加固，非重构）
  ├─ P3-10 per-quantum deadline 保证协作抢占收敛
  └─ P3-11 Scheduler.Shutdown 主动交接 lease
```

---

## 四、架构级张力与运维现实（非单点 bug，而是系统性权衡）

上面第二节是"可定位到行号的缺陷"。本节是**更高层的结构性张力**——它们不是某处写错了，而是"Agent 即进程"这个定位本身带来的固有代价，需要**架构决策**而非补丁来应对。综合评级 **工程复杂度 B- / 生产落地 B** 的理由就在这里。

### 🟠 A-1　状态机组合爆炸（combinatorial state space）

**实测状态全集**：
- 任务状态 **6 个**：`READY / LEASED / RUNNING / SUSPENDED / COMPLETED / FAILED`（`taskfabric/state.go:8-19`，合法迁移由 `canTransition` 白名单约束，`state.go:29`）。
  > ⚠️ 更正：早期口头评审里说的 `PENDING/CLAIMED/CANCELLED` 与实际枚举不符，以本行代码为准。
- agent 状态 **4 个**：`IDLE / RUNNING / SUSPENDED / RETIRED`（`agentfabric/agent.go:15-23`）。
- 再叠加 **Lease 的 epoch 版本**（fencing token）与 **Quantum 计数**（`task.go:36-42`，跨所有持有者累加）。

**张力**：三者交叉，一个任务的"真实运行态" = 任务状态 × 当前 owner 的 agent 状态 × lease epoch × 重试次数 × quantum 数。`aresrecovery` 的恢复逻辑（替换执行体、租约过期、winner 死在候选构建与执行之间——`scheduler.go:716-753` 的三档策略 release / release+nominate / 等 TTL）**已经在处理这个笛卡尔积的边角**。状态机本身设计得干净（有 `canTransition` 守门），但**组合空间的增长是超线性的**，每加一个维度（如未来加 `StateBlocked`、加抢占优先级）都会让恢复路径的分支数暴涨。

**应对建议**：
1. 把"任务态 × agent 态 × epoch"的**合法组合显式建模成一张表**（而非散落在 `if` 里），配一组穷举单测锁死非法组合。
2. 恢复逻辑用**状态迁移事件**驱动，而不是在 `executeWithCandidates` 里堆 `if`；否则 P0-2/P0-3 修完后这里的分支会更难维护。
3. 明确**不再增加状态维度**，除非 profiling 证明必要（呼应 `scheduler.go:437` 作者自己写的"per-agent 队列已删，除非证明竞争再引入"的克制）。

### 🟠 A-2　LLM 延迟 vs 量子粒度的根本矛盾

**实测**：调度器 drain 周期 `PollInterval = 500ms`（`scheduler.go:252`、`preemptInterval` 默认 `500ms`，`scheduler.go:408`）。一个量子 = 一次 LLM 调用 + 一轮工具执行。

**张力**：
- 若 LLM 响应 8~10s、工具执行数秒，则**一个量子 ≫ 500ms**，drain 循环绝大多数时间在等 I/O 或空转，"精细调度"退化成"粗粒度轮询"。
- 若把量子做小（如只到"LLM 返回 tool_calls"就 yield），则 yield/resume + checkpoint 落盘的**开销占比**吃掉收益，且一次工具执行被拆到两个量子，副作用原子性变复杂。
- 事件触发（`scheduler.go:361-370` 订阅 Created/Ready/Completed/Failed/Yielded）能缓解"空等 500ms"，但**缓解不了"量子本身很长"**——因为一个量子内 LLM hang 住时，事件通道帮不上忙（见 P3-10）。

**应对建议**：
1. 承认在 LLM 场景下**量子天然偏长**，把调度价值从"低延迟抢占"重新定位为"**高吞吐下的公平性 + 容错 + 资源配额**"——这三点即使量子长也成立，且是差异化卖点。
2. drain 周期改为**自适应**：有 READY 任务时事件驱动立即 drain（已部分实现），无任务时指数退避拉长间隔，减少空转。
3. 文档/对外叙事里**明确量子的时间尺度预期**（秒级而非毫秒级），避免使用者按 OS 调度器的直觉误用。

### 🔴 A-3　分布式调试地狱：trace 上下文断在 IPC 边界（可定位根因）

**实测**：
- `ares_observability/log.go` 确实给每次 LLM 调用打了 `trace_id`（`log.go:42/52/69/78/94`）。
- **但 `agentipc.Message` 只有 `CorrelationID`，没有 `TraceID` 字段**（`bus.go:23-24`；`primitives.go:163/274` 只盖 `CorrelationID`）。全仓 `agentipc` 包内**搜不到任何 tracer/TraceID 传播**。

**影响**：这正是"调试地狱"的**具体断点**——当 `chatCognition` 通过 `ask_agent`/`Delegate`/`Handoff` 把控制流交给另一个 agent 后，**trace_id 不随消息传递**。于是 5 个 agent 协作时，你手里有 N 条**互不关联**的 trace，`CorrelationID` 只能把"一次请求↔一次回复"配对，**无法把"一个 root 任务派生的整棵协作树"串成一条端到端链路**。跨进程失败（租约竞争、epoch fencing 拒写、DLQ 丢消息）时，无法用一个 ID 拉出全貌。

**修复建议（这条最该补，且改动可控）**：
1. 给 `agentipc.Message` 加 `TraceID string` 字段，`Send/Request` 时从 `ctx` 取（复用 `llm/client.go:253` 的 `c.tracer.GetTraceID(ctx)` 同源 tracer），`Reply` 回填。
2. 接收端 handler 用消息里的 `TraceID` **重建 ctx**，使下游 LLM 调用/工具执行/事件写入挂到同一 trace。
3. 让 `taskfabric` 的 `Origin`（`task.go:30-35`，已是 Kernel 校验的 provenance）与 `TraceID` 关联——`Origin` 给了**任务谱系**，`TraceID` 给**运行时链路**，两者拼起来才是完整的分布式可观测。
4. 与 P0-2（IPC 死信）合并设计：DLQ 记录里带 `TraceID`，丢消息可直接定位到是哪条链路断的。

### 🟢 A-4　适用边界：优势场景 vs 现实阻碍（B 评级的由来）

**真正适合**（这些场景里，"Agent 即进程"的容错/调度/配额价值压过复杂度成本）：
- 长时运行、需容错的多 agent 协作（代码重构、数据分析流水线、批量研究任务）；
- "规划-执行"严格分离的安全敏感环境（planner 不碰工具，见 `planner_cognition.go:76-79`）；
- 多租户资源配额与治理（`agentfabric` quota + `budgetOK/consumeBudget`，`scheduler.go:195-212`）。

**现实阻碍**（决定它暂时是"研究级内核"而非"开箱即用框架"）：
- **量子粒度矛盾**（A-2）：LLM 的秒级延迟天然削弱"精细调度"的收益。
- **Checkpoint 写压力**（P2-7）：对话历史全量序列化 + 高频 yield → 存储写放大 + 上下文窗口溢出风险，**且当前无压缩/增量 diff**。
- **调试成本**（A-3）：跨进程 IPC + 租约竞争 + epoch fencing 的排障复杂度是指数级的，而 trace 目前**恰好断在最关键的 IPC 边界**。

**结论**：B 不是"做得差"，而是"**为极端场景付了通用场景不必付的复杂度税**"。对目标场景（上面三条）这笔税划算；对"搭个 ReAct chatbot"则是杀鸡用牛刀。修补 A-3（trace 贯穿）和 P2-7（checkpoint 护栏）能显著降低落地摩擦，是把 B 抬到 A- 性价比最高的两刀。

---

## 五、给作者的一句话

代码里那 20 处 `TODO(tech-debt)` **几乎每一条都自带根因分析和修复方向**（如 P0-1 连"为什么不能纯用墙钟 grace"都想清楚了），这不是失控的代码库，而是一个**作者比谁都清楚边界在哪、只是还没时间收口**的系统。真正要做的不是"找 bug"，而是**把已写好的机制（Reaper、DLQ、summarizer、cost 惩罚）接上电**——上面 P0 三条，两条的实现骨架其实已经在仓库里躺着了。而第四节那些"架构张力"里，唯一能靠一次小改动（给 `agentipc.Message` 加 `TraceID` 并贯穿）就显著改善体验的，是 A-3 的调试链路——建议与 P0-2 的 IPC 死信**合并成一次 IPC 改造**一起做。
