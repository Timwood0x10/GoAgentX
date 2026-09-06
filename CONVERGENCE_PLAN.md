# ARES AgentOS 最终收敛计划（CONVERGENCE PLAN）

> **目标**：一条主线、一个内核总调度、一个 runtime 管 agent 全生命周期。
> **依据**：对真实 goagent 代码的逐包核验（非仅 codebase-memory 索引）。凡本计划引用的事实都以源码为准，索引规模（如 24,412 nodes / 138,787 edges）只作量级提示，不作契约。
> **硬约束**：本计划**不允许删除 `internal/workflow/engine` 的 `MutableDAG`**——它是全仓唯一的任务图，工具 DAG 主线（M0–M6）立身其上。

---

## 〇、当前状态（已核实的真实事实）

4 条并行主线 + 真实重复集：

| # | 主线 | 入口 | 代表包 | 问题 |
|---|---|---|---|---|
| A | ARES Runtime | `cmd/ares/main.go` | ares_runtime, ares_memory, ares_evolution | 最完整，被 arena/flight/eval 碎片化 |
| B | Agent Fabric | `internal/agentfabric/` | agentfabric, taskfabric | 核心最稳，与 A/C 双写生命周期 |
| C | Kernel/Scheduler | `internal/kernelscheduler/` `system_runtime/` `kernelctx/` | 新内核线 | 最新、最 divergent |
| D | Examples/Compat | `examples/*`（**33**，非 22）+ `compat/` + `api/` | 验证场景 + 兼容层 | 数量膨胀，无收敛目标 |

**核心矛盾（已核实，部分是"三套"而非两套）**：
1. **生命周期三套**：`ares_runtime.Manager`（`StartAgent/StopAgent/RestartAgent`，操作 `base.Agent`）+ `agentfabric.Fabric` + 遗留 leader 管线。
2. **进化两套**：`ares_evolution`(v1) vs `evolution`(v2)。—— 已定：**主线只留 v2 + `MutableDAG`**。
3. **记忆两套（有重叠，非全同物）**：`ares_experience`=任务结果→经验蒸馏；`ares_memory`=会话记忆+蒸馏管线。合并前须定边界，不是纯去重。
4. ~~两套 DAG~~ ⚠️ **更正为"一套图、两处消费"**：唯一的 `MutableDAG` 在 `internal/workflow/engine`；`taskfabric/dag.go` 是 `ReadyTasks()` 调度原语，不构成第二套 DAG。**不得按"删 workflow 留 taskfabric"执行。**

---

## 一、终极目标架构

```
cmd/ares/main.go  ← 唯一入口（1 个二进制）
        │
        ▼
┌─────────────────────────────────────────────┐
│ internal/kernel/      ← 唯一调度内核          │
│   scheduler · quantum · load_tracker         │
│   executor_registry · decision_recorder      │
│   fabric_executor · shadow · runtime(Orchestr)│
└──────────────┬──────────────────────────────┘
               │
┌──────────────▼──────────────────────────────┐
│ internal/fabric/     ← 唯一编排层             │
│   agent/   (agentfabric 生命周期)            │
│   task/    (taskfabric 任务投影)             │
│   workflow/engine/  = 唯一的 MutableDAG ★    │
│   planprojection/   = 图→任务增量编译         │
└──────┬──────────────────────────────┬──────┘
       │                              │
┌──────▼─────────┐          ┌─────────▼──────────┐
│ internal/kernel/ │          │ internal/runtime/   │
│  4 条主线共用     │          │  memory/ evolution/ │
│  内核上下文/IPC   │          │  protocol/ observab │
└─────────────────┘          └────────────────────┘
```

三条硬规则（不可协商）：
1. **一个内核**：`internal/kernel/` 是唯一调度器；所有 agent/task 的调度决策必须经它。
2. **一个 runtime**：`internal/runtime/` 管 agent 完整生命周期（Start → Process → Snapshot → Stop → Save → Restore），**不再有第二套**。
3. **一条主线 + 一张图**：`cmd/ares` 唯一入口；`internal/workflow/engine.MutableDAG` 唯一任务图（★），任何"另起一张图"的念头回本文档改。

---

## 二、判定标准（留主线 / 归档）

**留主线**（同时满足）：
- 有真实调用方（fan-in > 0，非仅测试）；
- 属 OS 原语（调度/IPC/上下文/资源）或核心编排（agent 生命周期 / 任务图 / 事件总线）；
- 被 `cmd/ares` 或 `sdk` 直接依赖。

**归档/删除**：
- fan-in = 0 且 fan-out = 0 的孤立包（examples 下 33 个目录大多在此）；
- 确属重复实现（两套进化、两套生命周期、两套 agent 接口）；
- 仅验证 demo（aresos-demo、runtime_evolution、peer-spawn-demo、collab-graphs 等）。

> 执行顺序：**先跑全仓 fan-in 审计出"留/归档"表，再做最小合并**，不要先搬家。

---

## 三、分阶段收敛

### Phase 0 — 冻结与盘点（不动代码）
- 合并三份并行文档 → 单一 `ARCHITECTURE.md`：`ARCHITECTURE_AND_DATAFLOW.md` + `ARES_PROJECT_REVIEW.md` + `TOOL_DAG_MAINLINE_DESIGN.md`。
- 冻结 `examples/`（**33** 目录），写"不再新增 demo 目录"约束。
- 跑 fan-in 审计，产出"留主线/归档"表。
- **验收**：`git log --oneline -5` 的 5 个 commit 不再产生新顶层目录；`ARCHITECTURE.md` 存根。

### Phase 1 — 内核单一化（唯一调度器）
**合并进 `internal/kernel/`**：
| 来源 | 去向 |
|---|---|
| `kernelscheduler/`（scheduler/quantum/load_tracker/executor_registry/decision_recorder/fabric_executor/shadow） | `internal/kernel/` |
| `kernelctx/` | `internal/kernel/ctx.go` |
| `system_runtime/`（component/orchestrator/registry/snapshot/state） | `internal/kernel/runtime.go` |

**⚠️ 去重纠正（不要做成错的）**：
- ❌ v1/v2 说"删 `aresrecovery.recovery.go:124 spawnAgent`，统一到 `agentsyscall.syscall.go:250 SpawnAgent`"——**这是范畴错误**：`Recovery.spawnAgent` 做 A1 执行体注入 + `SpawnForRecovery`（恢复专用）；`syscall.SpawnAgent` 是面向 LLM 的 Kernel API。两者都汇到 **`agentfabric.Spawn`**。**真正的收敛点是 `agentfabric.Spawn`**，recovery 保留它自己的恢复语义。
- `aresrecovery.RestartAgent` → 改为调 kernel 的 Restart（可）。

**验收**：`internal/kernel/` 存在；`kernelscheduler/ kernelctx/ system_runtime/` 三目录删除；`grep -r kernelscheduler` 0 命中；`go build ./...` 通过。

### Phase 2 — 编排层单一化（唯一 Fabric + 唯一图）
**合并进 `internal/fabric/`**：
| 来源 | 去向 |
|---|---|
| `agentfabric/` | `internal/fabric/agent/` |
| `taskfabric/` | `internal/fabric/task/` |
| **`workflow/engine/`（MutableDAG）** | **`internal/fabric/workflow/engine/`（唯一图，★ 不删）** |
| `planprojection/` | `internal/fabric/task/`（增量编译并入任务投影） |

**统一图（★ 关键）**：保留 `workflow/engine.MutableDAG` 为唯一任务图接口；`taskfabric.dag.go` 维持"依赖满足→Ready"的调度原语，不升级为第二套图。
**统一 agent 接口**：`internal/agents/base/agent.go` 只是三套之一，**先定为"主线 Agent 接口"再统一**，不要默认它是权威；`sdk/agent.go` 第二套改为薄包装。

**验收**：`internal/fabric/` 存在，`agentfabric/ taskfabric/` 删（workflow/engine 保留位移）；`grep "type MutableDAG struct"` 仍只命中 `internal/fabric/workflow/engine`；全仓 Agent 接口收敛为 1 个（先定）。`go test ./internal/fabric/...` 通过。

### Phase 3 — 运行时服务化（runtime 管完整生命周期）
**合并进 `internal/runtime/`**：
| 来源 | 去向 |
|---|---|
| `ares_memory/` + `ares_experience/` | `internal/runtime/memory/`（**先定边界：经验 vs 会话记忆**） |
| `ares_evolution/`(v1) + `evolution/`(v2) | `internal/runtime/evolution/`（**主线只留 v2**；v1 只是收割/不再新增） |
| `ares_mcp/` `ares_skills/` `ares_protocol/` | `internal/runtime/protocol/` |
| `ares_observability/` | `internal/runtime/observability/` |
| `ares_eval/` + `eval/` | `internal/runtime/eval/` |

**归档为 examples/benchmarks**：`ares_arena/`→`examples/arena/`、`ares_flight/`→`examples/flight/`、`ares_archive/`→`examples/archive/`。

**验收**：agent 生命周期闭环测试通过（serve→spawn→process→snapshot→stop→restore 重启一致）；`evolution run` 单命令触发完整闭环（C1–C7 + E1–E6）；`go test ./internal/runtime/...` 通过。

### Phase 4 — 入口单一化（唯一 CLI）
- `cmd/ares/main.go` 唯一入口；`cmd/ares/` 收敛为 `main/serve/agent/evolution/kernel/db/dashboard` 少数文件。
- `examples/*/main.go`（33）降级为 `examples/_fixtures/`。
- `compat/` 评估去留（过渡 shim，末尾删）。
**验收**：`grep -r "package main" cmd/ | wc -l` = 1；`ARES serve` 单命令启完整 OS。

### Phase 5 — 验证与文档
**验收**：顶层目录从 50+ → ≤15；`go vet ./...` 过；serve→agent list→evolution run→status→dashboard 全链路通；`ARCHITECTURE.md` 单一源 + ADR。

---

## 四、三条高危——不可当目录移动

1. **Merge = 重构，不是 mv**：合并包前按 fan-in 定去留，合并步骤拆小、每步 `go test` + linter 兜底，别一次性大搬家。
2. **不允许动 `workflow/engine.MutableDAG`**：Phase 2 若按"删 workflow 留 taskfabric"执行，等于删掉工具 DAG 主线（M0–M6 全断）。方向见 §三 Phase 2 ★。
3. **recovery 恢复语义不能汇入 LLM-facing syscall**：收敛点是 `agentfabric.Spawn`，recovery 保留 `SpawnForRecovery`（Phase 1 ⚠️）。

---

## 五、风险与回滚

| 风险 | 缓解 |
|---|---|
| Phase 2 合并破坏现有测试 | 每 Phase 结束打 tag，失败回滚上一 tag |
| recovery 删除影响恢复能力 | Phase 1 先写恢复集成测试再删 |
| 生命周期三套合一时误删某入口 | 以 fan-in 为准；`sdk` 与 `cmd/ares` 双依赖的才留主线 |
| 大重构波及工具 DAG 主线 | 主线里程碑单独保护：`workflow/engine` 与其测试不进收敛删除范围 |
| examples 降级后用户脚本失效 | `README` 更新入口说明 |