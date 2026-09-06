# ares Architecture Deep Dive (XXIII): The "Quant Trading Module" — An Honest Statement: It Does Not Exist in This Codebase (0.3.x)

> This article is different from every other in the series. It's not "look at this great architecture." It's "there was supposed to be an architecture here, and there isn't."
> Plainly: **the current repository contains no quantitative trading implementation code.** Everything you see labeled `internal/ares_quant`, `internal/quant`, and `examples/quant-trading` in the old version of this article, in `CAPABILITY-MAP`, in `ARCHITECTURE`, and even in a dedicated `docs/en/development/quant-trading.md` — **does not exist.**

---

## 一、The honest conclusion, up front

I grepped the entire repository. The result is unambiguous:

| What I expected to find | Actual result |
|-------------------------|---------------|
| `internal/ares_quant/` package | **does not exist** |
| `internal/quant/` package | **does not exist** |
| `examples/quant-trading/` demo | **does not exist** |
| `plan/quan/quant-implementation-plan.md` | **does not exist** |
| `internal/dashboard/` (cited by the old draft) | **does not exist** (deleted; see Deep Dive XVI) |
| Any `market/`, `marketmaking/`, `portfolio/`, `research/`, `indicators/`, `dataflow/`, `store/`, `marketmaking_api/` sub-package | **does not exist** |

If you grep `quant`, the hits are misleadingly unrelated:
- `internal/taskfabric/quantum.go` and `internal/kernelscheduler/quantum_hook.go` — these are **execution quanta** (a "one execution step" concept in DAG orchestration), nothing to do with trading.
- The word "quanta" in `docs/25-config-yaml-guide` is the same scheduling concept.
- `grep position` / `trading` / `strategy` hits resolve to `regex match positions`, `strategy_adapter.go` (an evolution strategy), and `progress` — all unrelated.

The old article claimed the quant module had **9,768 lines (~11% of the codebase)**, a market-making engine, portfolio risk metrics, a backtesting framework, research agents, Yahoo/CoinGecko/Polymarket data sources, SQLite storage, MCP tool registration… **None of that is backed by any code.** Those claims should be flagged **unverified（待核实）** — but the truth is stronger than "unverified": it's "verified absent."

---

## 二、So what about "quant" actually survives in the repo?

Not much — but one thing is real: **a document, not code.**

### 2.1 `docs/en/development/quant-trading.md` (real)

This is a **design guide / implementation plan** — literally titled "ares for Quant — 量化交易开发指南." It describes what **should be built**: 8 agent roles (fundamentals / sentiment / news / technical analysts, bull & bear researchers, trader, risk manager, portfolio manager), plus an architecture for `internal/quant/` (~1,850 lines) and `examples/quant-trading/` (~1,710 lines) with **line-count estimates**.

But the giveaway is that it's a blueprint, not a snapshot of reality: the interfaces it cites are all in deleted or nonexistent packages.

| Interface cited in the doc | Package path (as written) | Reality |
|----------------------------|---------------------------|---------|
| `dashboard.AgentRequest` / `orch.CreateAgent()` | `ares/internal/dashboard` | `internal/dashboard` **was deleted** |
| `graph.NewGraph()` | `ares/internal/workflow/graph` | real DAG lives in `internal/taskfabric` style; path is wrong |
| `internal/quant/market/polymarket.go` `FetchMarket` | — | file does not exist |
| `internal/quant/market/yahoo.go` | — | file does not exist |

The doc also references `plan/quan/quant-implementation-plan.md` — **that plan file does not exist either.** The best this document can serve as evidence of is: "we planned it, even wrote a blueprint, but either the code was never landed or it has since been removed."

### 2.2 The "unverified（待核实）" entries in `CAPABILITY-MAP` and `ARCHITECTURE`

- `docs/CAPABILITY-MAP.md` / `docs/CAPABILITY-MAP.en.md`:
  > Quantitative trading | `internal/ares_quant` | Market making, indicators, portfolio management, research

  A row that lists `internal/ares_quant`, yet **no such package exists in any code directory.** (待核实: it may describe an removed or never-merged version.)

- `docs/zh/ARCHITECTURE.md`:
  - draws `QUANT["internal/ares_quant<br/>Portfolio / Market / Research / MarketMaking / Indicators"]` in its diagram
  - tabulates `internal/ares_quant | 量化 | 投资组合模拟、市场数据、研究记忆`

  These diagrams and tables describe **modules on paper, not implemented modules.** There is no `ares_quant` in `internal/`.

### 2.3 The old article itself (real, but its content is not)

`docs/articles/zh/23-quant-trading.md` and `docs/articles/en/23-quant-trading.md` exist, but their content is fabricated — they describe a module that doesn't exist. This rewrite (and its Chinese counterpart) exists precisely to correct that.

---

## 三、Framework capabilities that genuinely exist (what a quant build could rest on)

To avoid misleading people, let me be precise about which ares capabilities cited by the quant blueprint **are real**:

| ares capability | Real package | Note |
|-----------------|--------------|------|
| `EventStore` | `internal/ares_events` | real, `Append/Read/Subscribe` |
| Arena fault injection | `internal/ares_arena` | real, wired to the Flight Recorder via `FlightBridge` |
| Memory distillation | `internal/ares_memory` | real |
| MCP tool registry | `internal/ares_mcp` / `tools` | real |
| DAG workflows | `internal/taskfabric` | real, includes `quantum.go` (note: execution *quanta*, not trading) |

In other words: **nothing stops you from building a quant research system on ares — the framework's capabilities are all here — but the ares repo itself ships no trading logic.** To use it you'd build from the blueprint in `docs/en/development/quant-trading.md`, not import a ready-made `internal/ares_quant`.

---

## 四、What this says (a lesson, not an excuse)

I came to write "here's the quant module we built." To write it truthfully I actually went and read the code — and found there was none. That alone is a loud wake-up call, worth writing down:

1. **Docs and reality drift apart — faster than you'd think.** `CAPABILITY-MAP`, `ARCHITECTURE`, and the old article still talk about a module that was removed or never landed. When the paths those docs cite (`internal/dashboard`) are also gone, the drift compounds.
2. **The flip side of "experiments should be labeled as experiments" is this: a module that doesn't exist shouldn't masquerade in the docs as if it does.** Blueprints and current state must be labeled separately. This rewrite strikes the "fake-existent" module off the capability list, restoring it to an honest "planned/removed" state.
3. **The framework's real value is what deserves the spotlight.** ares' solid core is Runtime + Workflow + Memory + Events + Flight Recorder. Letting a "quant demo" impersonate a core capability only makes people who try to use it come up empty.

**The most honest closing line**: the current (0.3.x) ares repository **has no quantitative trading module.** If you see `internal/ares_quant` written anywhere, it's either stale documentation or an as-yet-unlanded blueprint. The codebase has no such thing.

---

### Appendix: verify it yourself

```bash
cd /Users/scc/go/src/goagent
find . -type d -name "*quant*"                              # only quantum.go scheduling code, no ares_quant
grep -rli "quant\|marketmaking" internal/ examples/        # hits are quantization/quantum/unrelated
grep -ri "ticker\|sharpe\|drawdown\|backtest\|polymarket\|coingecko" internal/ examples/  # no trading code
ls examples/   # no quant-trading
```

*Every other architecture article in this series describes code that actually exists; this one (article 23) is the series' honest-disclaimer exception.*