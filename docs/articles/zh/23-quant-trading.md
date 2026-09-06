# ares 架构拆解 (XXIII)：量化"交易模块"——诚实声明：它在代码里不存在（0.3.x）

> 这篇文章和其他文章不同。别的文章说"看这个伟大的架构"，这篇是说"这里本来应该有个架构，但它没了"。
> 坦白讲：**当前仓库里没有任何量化交易实现代码。** 你在旧的这篇文章、`CAPABILITY-MAP`、`ARCHITECTURE`、甚至专门的一份 `docs/en/development/quant-trading.md` 里看到的 `internal/ares_quant`、`internal/quant`、`examples/quant-trading`——**都不存在。**

---

## 一、最诚实的结论先行

我去 grep 了整个仓库。结论非常明确，没有任何含糊：

| 我想找到的 | 实际结果 |
|-----------|---------|
| `internal/ares_quant/` 包 | **不存在** |
| `internal/quant/` 包 | **不存在** |
| `examples/quant-trading/` demo | **不存在** |
| `plan/quan/quant-implementation-plan.md` | **不存在** |
| `internal/dashboard/`（旧文拿来举例的） | **不存在**（已被删除，见系列第十六篇） |
| 任何 `market/`、`marketmaking/`、`portfolio/`、`research/`、`indicators/`、`dataflow/`、`store/`、`marketmaking_api/` 子包 | **不存在** |

如果你搜 `quant`，命中的都是误导性弱相关：
- `internal/taskfabric/quantum.go` / `internal/kernelscheduler/quantum_hook.go` —— 这是**调度量子（execution quantum）**，是 DAG 编排里"一个执行步"的概念，跟量化交易一毛钱关系都没有。
- `docs/25-config-yaml-guide` 里的 "quanta" 是同一个调度量子概念。
- grep `position` / `trading` / `strategy` 命中的是 `regex position`（正则匹配位置）、`strategy_adapter.go`（进化策略）、`progress` 之类，全是无关代码。

旧文声称量化模块有 **9,768 行、约占代码库 11%**、包含做市引擎、投资组合指标、回测框架、研究 Agent、YAML/CoinGecko/Polymarket 数据源、SQLite 存储、MCP Tool 注册……**这些描述没有任何对应代码支撑。** 全盘标记**（待核实）**，而真相是：不是"待核实"，是"经核实不存在"。

---

## 二、那么"量化"这件事，留在仓库里的到底是什么

不多，但确实有一份真实存在的东西——**文档，而不是代码**：

### 2.1 `docs/en/development/quant-trading.md`（真实存在）

这是一份**设计指南 / 实施计划**，标题就是"ares for Quant — 量化交易开发指南"。它描述的是一个**应该被建成**的东西：8 个 Agent 角色（基本面/情绪/新闻/技术分析师、多空研究员、交易员、风控、组合经理），一份 `internal/quant/` 的架构（约 1,850 行）+ `examples/quant-trading/`（约 1,710 行）的**行数预估**。

但关键在于：**这份文档本身就是蓝图，不是现状。** 证据是它引用的接口全是已删除或不存在的包：

| 文档里引用的接口 | 包路径（文档写的） | 现实 |
|-----------------|-------------------|------|
| `dashboard.AgentRequest` / `orch.CreateAgent()` | `ares/internal/dashboard` | `internal/dashboard` **已被删除** |
| `graph.NewGraph()` | `ares/internal/workflow/graph` | 实际是 `internal/taskfabric` 风格的 DAG，路径不对 |
| `internal/quant/market/polymarket.go` 的 `FetchMarket` | — | 文件不存在 |
| `internal/quant/market/yahoo.go` | — | 文件不存在 |

这份文档最后还引用了 `plan/quan/quant-implementation-plan.md`——**这个计划文件也不存在。** 你要把这份文档当证据，也只能当"我们曾经打算做，甚至写好了蓝图，但代码从没落地（或已被移除）"的证据。

### 2.2 `CAPABILITY-MAP` 与 `ARCHITECTURE` 里的"待核实"条目

- `docs/CAPABILITY-MAP.md` / `docs/CAPABILITY-MAP.en.md`：
  > 量化交易 | `internal/ares_quant` | 做市、指标、组合管理、研究
  
  这行白纸黑字列了个 `internal/ares_quant`，但**没有任何代码包里存在这个包**。（待核实：它可能描述的是一个已删除或从未合并的版本。）

- `docs/zh/ARCHITECTURE.md`：
  - 架构图里画了 `QUANT["internal/ares_quant<br/>Portfolio / Market / Research / MarketMaking / Indicators"]`
  - 模块表里写 `internal/ares_quant | 量化 | 投资组合模拟、市场数据、研究记忆`

  这些图和表是**图纸上的模块**，不是已实现的模块。真实的 `internal/` 目录里没有 `ares_quant`。

### 2.3 旧文章本身（真实存在，但内容不真实）

`docs/articles/zh/23-quant-trading.md` 和 `docs/articles/en/23-quant-trading.md` 存在，但内容是虚构的——它们描述了一个不存在的模块。本文（及英文对应篇）就是为纠正这一点而重写的。

---

## 三、框架层面：哪些"被量化文档用到"的能力是真实存在的

为了不误导，我把"量化蓝图在框架里能靠什么搭起来"的相关点也说清楚——这些是**真实存在**的框架能力：

| ares 能力 | 真实包 | 说明 |
|-----------|--------|------|
| 事件存储 `EventStore` | `internal/ares_events` | 真实存在，`Append/Read/Subscribe` |
| Arena 混沌故障注入 | `internal/ares_arena` | 真实存在，与 Flight Recorder 通过 `FlightBridge` 打通 |
| 记忆蒸馏 `Memory Distillation` | `internal/ares_memory` | 真实存在 |
| MCP 工具注册 | `internal/ares_mcp` / `tools` | 真实存在 |
| DAG 工作流 | `internal/taskfabric` | 真实存在，含 `quantum.go`（注意：是调度量子） |

也就是说：**"用 ares 去写一个量化研究系统"这件事在技术上没有障碍——框架的能力都在，但 ares 仓库本身并没有内置任何交易逻辑。** 想用，就得照着 `docs/en/development/quant-trading.md` 那份蓝图从零搭，而不是 import 一个现成的 `internal/ares_quant`。

---

## 四、这说明了什么（教训，而不是辩解）

我本来准备写一篇"看，我们建了个量化模块"的文章。为了这篇重写，我老老实实去翻了代码——然后发现它不存在。这本身就是一记很响的警钟，值得写下来：

1. **文档和实物会脱节，而且比想象中快。** `CAPABILITY-MAP`、`ARCHITECTURE`、旧文章还在写一个已经被移除/从未落地的模块。当文档引用的路径（`internal/dashboard`）也已被删时，这种脱节会连环放大。
2. **"实验应该被标为实验"这句话，反过来也成立：不存在的模块，就不该留在文档里假装存在。** 图纸和现状必须分开标注。这篇重写就是把这个"假存在"的模块从能力清单里划掉，还原成一个诚实的"规划/已移除"状态。
3. **框架自带的真实价值，才是该宣传的。** ares 真正扎实的是 Runtime + Workflow + Memory + Events + Flight Recorder。让"量化 demo"冒充核心能力，只会让想用它的人扑空。

**最诚实的收尾**：当前版本（0.3.x）的 ares 仓库里**没有量化交易模块**。你如果看到别处写着 `internal/ares_quant`——那要么是旧文档的残留，要么是一份尚未落地的蓝图。代码仓库里没有它。

---

### 附录：验证方法（你可以自己跑）

```bash
cd /Users/scc/go/src/goagent
find . -type d -name "*quant*"                              # 只有 quantum.go 等调度代码，无 ares_quant
grep -rli "quant\|marketmaking" internal/ examples/        # 命中的全是 quantization/quantum/无关词
grep -ri "ticker\|sharpe\|drawdown\|backtest\|polymarket\|coingecko" internal/ examples/  # 无交易代码
ls examples/   # 无 quant-trading
```

*本站点其余架构文章描述的是真实存在的代码；本篇（第 23 篇）是系列里的一个"诚实免责声明"特例。*