# ARES config.yaml 配置指南（中文版）（0.3.x）

> 版本：0.3.x · 依据 `configs/ares.yaml` 实样与 `internal/ares_config` 字段名整理
> 英文版见 [English Version](./25-config-yaml-guide.en.md)

本指南说明如何编写 `ares.yaml`（或任意 `<name>.yaml`）来配置 ARES Runtime。
配置采用 **YAML + 强类型校验**。顶层 `ares_config.Config` 有 17 个 section；所有字段都有合理默认值，只设置你需要覆盖的项即可（零值哲学）。默认值以 `internal/ares_config/config_defaults.go` 为准。

**最小可用配置**（只需 LLM 即可启动，参考 `configs/ares.minimal.yaml`）：

```yaml
llm:
  provider: ollama        # ollama | openai | anthropic | openrouter
  model: llama3.2
  # api_key: ""           # 本地 provider 可留空；云端 provider 可用环境变量
  # base_url: ""          # 自定义 endpoint（可选）
```

**启动方式**（两套，别混）：

```go
// serve 侧：ares_config.Config —— 走 cmd/ares serve，字段最全
// sdk 侧：sdk.ConfigFile —— 走 LoadConfigFile → ToOptions → NewRuntime
cfg, _ := sdk.LoadConfigFile("ares.yaml")   // sdk/config.go
opts, _ := cfg.ToOptions()                  // 配置 → SDK Option
rt := sdk.NewRuntime(opts...)
// 或纯代码：sdk.NewRuntime(sdk.WithOllama("llama3.2"))
```

> 注意：sdk 侧的 `sdk.ConfigFile` 字段**少于** serve 侧 `ares_config.Config`（见 22 篇）。两处 shape 不同，下面逐节标注该字段属于哪一侧。

---

## 1. LLM（大模型接入）—— 两侧都有

`ares_config.LLMConfig`（serve）与 `sdk.LLMFileConfig`（sdk）字段：

```yaml
llm:
  provider: openai          # 必填：ollama | openai | anthropic | openrouter
  model: gpt-4o-mini        # 模型名
  api_key: ""               # API Key（或走 LLM_API_KEY / OPENAI_API_KEY / ANTHROPIC_API_KEY 等）
  base_url: ""              # 自定义 base URL（代理/私有部署）
  # serve 侧额外字段：
  timeout: 60               # 请求超时（秒），默认 60
  max_tokens: 4096          # 响应最大 tokens，默认 4096
  max_prompt_length: 8192   # 最大提示词字符数，默认 8192
  extra: {}                 # provider 专属扩展 KV
  fallbacks: []             # 失败时按序接管的各 LLMConfig（failover）
  # sdk 侧额外字段：
  temperature: 0.7          # 生成温度 [0,2]，默认 0.7（serve llm 解析无 temperature）
```

| provider | 说明 | 默认 model（未显式给，sdk.ConfigFile） |
|---|---|---|
| `ollama` | 本地 Ollama，/api/chat | `llama3.2` |
| `openai` | OpenAI / 兼容服务 | `gpt-4o-mini` |
| `anthropic` | Claude | `claude-3-haiku` |
| `openrouter` | OpenRouter 聚合 | `openai/gpt-4o-mini` |

校验（validateLLM）：provider 必须属于上面四者；serve 侧 `timeout`/`max_tokens` 必须为正。

> 已核实：`configs/ares.yaml` 只在注释里定义 `llm`，实样启用了 `provider: ollama`、`model: llama3.2`，并明确 `temperature`/`max_tokens` 注释。

---

## 2. Memory / 蒸馏 / RAG —— 两侧字段略有差异

serve 侧 `MemoryConfig`（含嵌套）：

```yaml
memory:
  # enabled: 三态开关 *bool：缺省即开启；显式 false 才关（serve 侧）
  # max_history: 闭环上下文保留轮数，默认 10
  # enable_distillation: 默认开启（*bool，nil=true）；显式 false 关闭
  # distillation_threshold: 每 N 轮触发一次蒸馏，0 = 不过门（每事件触发），默认 3
  # enable_rag: 默认 false（opt-in）
  # rag_top_k: RAG 检索片段数，默认 5（仅 enable_rag 时生效）
  # rag_min_score: 最小相似度阈值，默认 0.4（仅 enable_rag 时生效）
  session:
    enabled: true
    max_history: 50         # session 存储窗口，默认 50
  user_profile:
    enabled: true
    storage: memory         # "memory"或"postgres"
    vector_db: false
  task_distillation:
    enabled: true
    storage: memory
    vector_store: false
    prompt: ""              # 缺省用 DefaultTaskDistillationPrompt
    threshold: 0            # 事件订阅路径累积轮数，0 = 不过门
  archive:
    enabled: true           # 三态，默认开
    dir: .context/rounds    # 默认
    max_rounds: 200         # 默认
```

sdk 侧 `MemoryFileConfig` 只暴露平铺字段：

```yaml
memory:
  enabled: true                     # sdk 侧默认 false（与 serve 侧相反，需显式 true）
  max_history: 50                   # 0 = 组件默认
  max_sessions: 100                 # 0 = 组件默认
  enable_distillation: true         # 三态，nil 默认开
  distillation_threshold: 3         # 0 = 不过门
  enable_rag: false                 # opt-in
  rag_top_k: 5                      # enable_rag 时须 >= 1
  rag_min_score: 0.4                # [0,1]，enable_rag 时校验
```

> 诚实提醒：`configs/ares.yaml` 实样把记忆关掉了（`enabled: false`），并给出 `distillation_threshold: 3`、`enable_rag: false`、`rag_top_k: 5`、`rag_min_score: 0.4` 的示例。

---

## 3. 知识图谱（Knowledge / AKG）—— 依据 configs/ares.yaml

`knowledge` 块控制 AKG 检索与事实质量门。`configs/ares.yaml` 实样（注释）唯一启用项：

```yaml
knowledge:
  # chunk_size: >0 时自动启动并接入 RAG 管线的 doc 分块（sdk 用它触发）
  # chunk_size: 512
  # chunk_overlap: 64
  # top_k: 检索返回条数，默认 5
  # min_score: 最小相似度，默认 0.4
  quality:                      # AKG 质量门
    min_extraction: 0.5
    min_consistency: 0.5
    min_final_score: 0.55
    max_facts_per_ingest: 50
    enable_dedup: true
    dedup_threshold: 0.85
  embedding:                    # 向量化（写侧+读侧）
    model: "intfloat/e5-large-v2"
    base_url: "http://localhost:8000"
```

校验（sdk `validateKnowledge`）：chunk_size>0 时要求 chunk_overlap 在 `[0, chunk_size)`、top_k>=1、min_score 在 `[0,1]`；quality 仅 `min_final_score > 0` 时触发，各分数校验范围。

---

## 4. 遗传进化（GA Evolution）

`sdk.ConfigFile` 只暴露 `evolution.enabled`；serve 侧 `EvolutionConfig` 全套。`configs/ares.yaml` 仅启用 `enabled: false`，其余在注释里给出默认（与 `config_defaults.go` 一致）：

```yaml
evolution:
  enabled: false                # 默认 false：不开则整条进化管线跳过
  # 基础 GA（serve 侧）：
  # population_size: 20
  # elite_count: 2
  # survival_rate: 0.6          # [0,1]
  # mutation_rate: 0.2
  # min_mutation_rate: 0.05
  # max_mutation_rate: 0.5
  # generations: 15             # 0 解释见文 22；校验要求 >=1
  # breeding_pool_ratio: 0.5
  # min_interval: "5m"          # Go duration 格式
  # selection_strategy: "tournament"  # tournament | rank | roulette | sus | truncation | random
  # tournament_size: 3
  # crossover_type: "uniform"   # uniform | two_point | segment
  # target_fitness: 0
  # steady_state: false
  # steady_state_replace_rate: 0.3
  # llm_scoring: { enabled: false, seed: 0, max_calls_per_generation: 100 }
  # 控制面（均选填，0/缺省回退代码默认）：
  # lifecycle: { fitness_window: 50, min_samples_before_judge: 10, cold_start_score: 0.5,
  #             watch_interval: "30s", min_active_duration: "90s", outcome_weight, dimension_eval_weight,
  #             workflow_weight, scheduler_weight, recovery_weight, blacklist_generations: 3 }
  # rollback: { enabled: true, degradation_threshold: 0.15, window_size: 5, min_samples: 3 }
  # shadow: { min_samples: 20, min_win_rate: 0.55, replay_window_span: "10m", replay_query_limit: 200 }
  # shadow_execution: { enabled: false, sample_size: 3 }
  # channel_feedback: { collab_enabled: false, collab_weight: 0.0, tool_enabled: false, tool_weight: 0.0 }
  # tool_projection: { enabled: false, interval: "10m", min_samples: 3 }
  # gates: { eval_min_score: 0.7, require_manual_approval: false, eval_suite: "" }
  # tool_pool: []              # 工具白名单候选集
  # guardrails: { max_tools_enabled: 0, require_any_tool: false, known_tools: [] }
  # deployment: ...            # 演进补丁的安全投产管线（deployment.DeploymentConfig）
```

校验（serve `validateEvolution`）：`population_size>=2`、`elite_count in [0, pop)`、`survival_rate in (0,1]`、`mutation_rate in [0,1]`、`generations>=1`；`tool_projection` 启用时 `interval>0`、`min_samples>=0`。

> 诚实提醒：GA 引擎内部（fitness 聚合、dream cycle、gates 语义）细节超出 `internal/ares_config` 字段面，不在此展开；上述仅列 YAML 可配置面。

---

## 5. Agents / Kernel / Tools / Reflection

`configs/ares.yaml` 实样：

```yaml
agents:
  # C1 扁平 peer 结构（默认，无 leader）：每个条目一个平等 agent
  peers:
    - id: coder
      capabilities: ["code", "refactor"]   # 全量声明能力集
      # priority: 1.0                      # 调度优先级，>=0
      # role: ""                           # agent profile id（W4）
      # max_tool_rounds: 0                 # 工具调用轮数上限，默认 5
    - id: reviewer
      capabilities: ["review", "audit"]
    - id: researcher
      capabilities: ["research"]
  # sub: [...]   # 旧 leader/sub 时代列表，作为 fallback 仍被接受

kernel:
  # loop_round_quanta: 1       # 一个 loop round 的调度量子数
  # loop_max_iterations: 0     # round 时钟上限，0 = 无限
  # policy: "taskfabric"       # taskfabric | legacy（默认 taskfabric）
  # lease_ttl: "5m"            # 任务租约时长（如 "45s"）

tools:
  builtin: true               # 内置工具集（calculator, web_search, file 等）
  # mcp:                       # stdio MCP 服务器命令列表
  #   - npx @modelcontextprotocol/server-filesystem ./data

reflection:
  enabled: false              # agent 自我反思开关
```

serve 侧额外：`kernel.policy`/`lease_ttl`/`resources`/`max_restarts`/各类 interval 与 timeout；`agents.sub` 每个条目 `{id, type, category, triggers, model, provider, dependencies, role, priority, max_tool_rounds, ...}`。

---

## 6. 存储 / Embedding / 认证

```yaml
# --- serve 侧 StorageConfig ---
storage:
  enabled: true
  type: postgres               # postgres | sqlite
  host: localhost
  port: 5432
  username: ares
  password: ""                 # json:"-" 防止 JSON 序列化泄漏
  database: ares
  ssl_mode: disable
  pgvector:
    enabled: false
    dimension: 1536
    table_name: embeddings

# --- sdk 侧 DatabaseFileConfig（configs/ares.yaml 用 database 键）---
database:
  host: localhost
  port: 5432
  user: ares
  password: ""
  database: ares
  ssl_mode: disable            # disable | require | verify-full

embedding:
  service_url: "http://localhost:8000/v1/embeddings"  # OpenAI 兼容
  model: "text-embedding-ada-002"

# --- serve 侧 SecurityConfig（JWT/RBAC）---
security:
  jwt_secret: ""               # 建议走 ARES_JWT_SECRET，勿提交 YAML
  jwt_expiry: "24h"
  auth_enabled: false
```

> 重要命名差异：**serve 侧用 `storage`，sdk 侧用 `database`**——`configs/ares.yaml` 两种都有（上面各自贴的是对应键）。写入时按你走的路径选。

---

## 完整示例（features 全覆盖，`configs/ares.yaml` 匿名块）

```yaml
llm:
  provider: openai
  model: gpt-5.6
  api_key: ${OPENAI_API_KEY}

memory:
  enabled: true
  enable_distillation: true
  distillation_threshold: 3
  enable_rag: true
  rag_top_k: 5
  rag_min_score: 0.4

knowledge:
  chunk_size: 512
  chunk_overlap: 64
  top_k: 5
  min_score: 0.4
  quality:
    min_extraction: 0.5
    min_consistency: 0.5
    min_final_score: 0.55
    max_facts_per_ingest: 50
    enable_dedup: true
    dedup_threshold: 0.85
  embedding:
    model: "intfloat/e5-large-v2"
    base_url: "http://localhost:8000"

evolution:
  enabled: true

tools:
  builtin: true

database:
  host: localhost
  port: 5432
  user: postgres
  password: REPLACE_WITH_YOUR_PASSWORD
  database: ares

embedding:
  service_url: http://localhost:8080/v1/embeddings
  model: text-embedding-ada-002
```

---

## 配置来源与优先级

配置可来自**多个来源**，合并进 `ares_config.Config`（serve）或 `sdk.ConfigFile`（sdk）：

serve：`Config` 结构体 → `setDefaults()` → YAML → `LoadFromEnv()`（`SERVER_*`/`LLM_*`/`DB_*`/`ARES_*`）→ 程序化 Options。后者覆盖前者，零值字段回退组件默认。

sdk：`LoadConfigFile` 读 YAML → `Validate` → `ToOptions` 转 `sdk.Option`；API key 缺省才回退 `resolveAPIKey` 绑定的环境变量。

> 诚实提醒：旧文"十二个来源"的三态优先级表格无法在代码里一一对应核实，故本文不照搬；环境变量的确切名单见 `internal/ares_config/config.go` 的 `LoadFromEnv`（22 篇已列）。

## 相关文档

- 配置系统深入：`docs/articles/zh/22-config-system.md`
- 完整示例：`examples/01-quickstart/`、`examples/12-yaml-driven-flags/`；最小起跑参考 `configs/ares.minimal.yaml`