# Fan-In 审计表（Phase 0 产出）

> 生成时间：2026-09-06
> 方法：`grep -rn` 非测试 `.go` 文件中 import 路径计数，按被引用频次降序。
> 用途：Phase 1–4 合并时确定"去留"——高 fan-in 包移动时影响面大，需分步拆。

## 内核三件套（Phase 1 合并目标）

| 包 | 非测试引用数 | 合并去向 |
|---|---|---|
| `internal/kernelscheduler` | 7 | `internal/kernel/` |
| `internal/kernelctx` | 6 | `internal/kernel/ctx.go` |
| `internal/system_runtime` | 4 | `internal/kernel/runtime.go` |

## 编排三件套（Phase 2b 合并目标）

| 包 | 非测试引用数 | 合并去向 |
|---|---|---|
| `internal/agentfabric` | 21 | `internal/fabric/agent/` |
| `internal/taskfabric` | 25 | `internal/fabric/task/` |
| `internal/planprojection` | 待定 | `internal/fabric/task/` |

## 运行时服务（Phase 3 合并目标）

| 包 | 非测试引用数 | 合并去向 |
|---|---|---|
| `internal/ares_memory` | 12 | `internal/runtime/memory/` |
| `internal/ares_experience` | 12 | `internal/runtime/memory/`（同模块分 API） |
| `internal/ares_evolution` (v1) | 27 | `internal/runtime/evolution/`（冻结） |
| `internal/evolution` (v2) | 待定 | `internal/runtime/evolution/`（推进） |
| `internal/ares_mcp` + `ares_skills` + `ares_protocol` | 5+5+待定 | `internal/runtime/protocol/` |
| `internal/ares_observability` | 11 | `internal/runtime/observability/` |
| `internal/ares_eval` + `eval` | 待定 | `internal/runtime/eval/` |

## 归档目标（Phase 3/4）

| 包/目录 | 合并去向 |
|---|---|
| `internal/ares_arena/` | `examples/arena/` |
| `internal/ares_flight/` | `examples/flight/` |
| `internal/ares_archive/` | `examples/archive/` |

## 高 fan-in 基础设施（不动）

| 包 | 非测试引用数 | 处置 |
|---|---|---|
| `internal/errors` | 81 | 不动 |
| `internal/logger` | 59 | 不动 |
| `internal/ares_events` | 57 | 不动 |
| `internal/knowledge` | 42 | 不动 |
| `internal/evidence` | 30 | 不动 |
| `internal/ares_config` | 30 | 不动 |

## examples 冻结约束

- `examples/` 当前 **34** 个目录（含 README.md）
- **冻结规则**：不再新增 demo 目录；现有目录在 Phase 4 降级为 `examples/_fixtures/`
- 新功能演示走 `docs/cookbook/` 或 `docs/articles/`
