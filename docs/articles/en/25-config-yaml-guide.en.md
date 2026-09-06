# ARES config.yaml Guide (English) (0.3.x)

> Version: 0.3.x · Field names follow `configs/ares.yaml` and `internal/ares_config`
> 中文版见 [Chinese Version](./25-config-yaml-guide.zh.md)

This guide explains how to write `ares.yaml` (or any `<name>.yaml`) to configure the ARES Runtime.
Configuration is **YAML + strongly typed validation**. The top-level `ares_config.Config` has 17 sections; every field has a sensible default — set only what you need to override (zero-value philosophy). Defaults follow `internal/ares_config/config_defaults.go`.

**Minimal working config** (LLM only is enough; see `configs/ares.minimal.yaml`):

```yaml
llm:
  provider: ollama        # ollama | openai | anthropic | openrouter
  model: llama3.2
  # api_key: ""           # empty for local providers; cloud providers can use env vars
  # base_url: ""          # custom endpoint (optional)
```

**Bootstrap options** (two paths, don't mix):

```go
// serve side: ares_config.Config — via cmd/ares serve, most fields
// sdk side:   sdk.ConfigFile  — via LoadConfigFile → ToOptions → NewRuntime
cfg, _ := sdk.LoadConfigFile("ares.yaml")   // sdk/config.go
opts, _ := cfg.ToOptions()                  // config → SDK Options
rt := sdk.NewRuntime(opts...)
// or pure code: sdk.NewRuntime(sdk.WithOllama("llama3.2"))
```

> Note: the `sdk.ConfigFile` shape has **fewer fields** than the serve-side `ares_config.Config` (see article 22). The two differ; each section below notes which side owns the fields.

---

## 1. LLM (Model Provider) — both sides

`ares_config.LLMConfig` (serve) and `sdk.LLMFileConfig` (sdk) fields:

```yaml
llm:
  provider: openai          # required: ollama | openai | anthropic | openrouter
  model: gpt-4o-mini        # model name
  api_key: ""               # API key (or LLM_API_KEY / OPENAI_API_KEY / ANTHROPIC_API_KEY…)
  base_url: ""              # custom base URL (proxy / private deployment)
  # serve-side only:
  timeout: 60               # request timeout in seconds, default 60
  max_tokens: 4096          # max response tokens, default 4096
  max_prompt_length: 8192   # max prompt chars, default 8192
  extra: {}                 # provider-specific KV extensions
  fallbacks: []             # LLMConfigs tried in order on failure (failover)
  # sdk-side only:
  temperature: 0.7          # [0,2], default 0.7 (serve llm parsing has no temperature)
```

| provider | Notes | Default model when unset (sdk.ConfigFile) |
|---|---|---|
| `ollama` | Local Ollama, /api/chat | `llama3.2` |
| `openai` | OpenAI / compatible | `gpt-4o-mini` |
| `anthropic` | Claude | `claude-3-haiku` |
| `openrouter` | OpenRouter aggregation | `openai/gpt-4o-mini` |

Validation (validateLLM): provider must be one of the four above; serve-side `timeout`/`max_tokens` must be positive.

> Verified: `configs/ares.yaml` defines `llm` in comments and enables `provider: ollama`, `model: llama3.2`, with `temperature`/`max_tokens` noted in comments.

---

## 2. Memory / Distillation / RAG — fields differ slightly between sides

Serve-side `MemoryConfig` (with nesting):

```yaml
memory:
  # enabled: tri-state *bool: absent means on; explicit false turns off (serve)
  # max_history: closed-loop turns kept, default 10
  # enable_distillation: on by default (*bool, nil=true); explicit false disables
  # distillation_threshold: fire distillation every N rounds, 0 = ungated (every event), default 3
  # enable_rag: default false (opt-in)
  # rag_top_k: retrieved snippets, default 5 (only when enable_rag)
  # rag_min_score: min similarity, default 0.4 (only when enable_rag)
  session:
    enabled: true
    max_history: 50         # session store window, default 50
  user_profile:
    enabled: true
    storage: memory         # "memory" or "postgres"
    vector_db: false
  task_distillation:
    enabled: true
    storage: memory
    vector_store: false
    prompt: ""              # default DefaultTaskDistillationPrompt when empty
    threshold: 0            # event-path accumulation rounds, 0 = ungated
  archive:
    enabled: true           # tri-state, on by default
    dir: .context/rounds    # default
    max_rounds: 200         # default
```

SDK-side `MemoryFileConfig` exposes flat fields only:

```yaml
memory:
  enabled: true                     # sdk side defaults false (opposite of serve; set explicitly)
  max_history: 50                   # 0 = component default
  max_sessions: 100                 # 0 = component default
  enable_distillation: true         # tri-state, nil defaults on
  distillation_threshold: 3         # 0 = ungated
  enable_rag: false                 # opt-in
  rag_top_k: 5                      # must be >= 1 when enable_rag
  rag_min_score: 0.4                # [0,1], validated when enable_rag
```

> Honest note: `configs/ares.yaml` currently disables memory (`enabled: false`) and shows `distillation_threshold: 3`, `enable_rag: false`, `rag_top_k: 5`, `rag_min_score: 0.4` as examples.

---

## 3. Knowledge Graph (Knowledge / AKG) — per configs/ares.yaml

The `knowledge` block drives AKG retrieval and the fact-quality gate. `configs/ares.yaml` examples:

```yaml
knowledge:
  # chunk_size: doc chunking (triggers AKG wiring + RAG when > 0; sdk uses it to trigger)
  # chunk_size: 512
  # chunk_overlap: 64
  # top_k: retrieval count, default 5
  # min_score: min similarity, default 0.4
  quality:                      # AKG quality gate
    min_extraction: 0.5
    min_consistency: 0.5
    min_final_score: 0.55
    max_facts_per_ingest: 50
    enable_dedup: true
    dedup_threshold: 0.85
  embedding:                    # vectorization (write + read side)
    model: "intfloat/e5-large-v2"
    base_url: "http://localhost:8000"
```

Validation (sdk `validateKnowledge`): when chunk_size>0 require `chunk_overlap in [0, chunk_size)`, `top_k >= 1`, `min_score in [0,1]`; quality fires only when `min_final_score > 0`, each score range-checked.

---

## 4. Genetic Evolution (GA)

`sdk.ConfigFile` exposes only `evolution.enabled`; serve-side `EvolutionConfig` is the full set. `configs/ares.yaml` enables only `enabled: false`, with defaults in comments (matching `config_defaults.go`):

```yaml
evolution:
  enabled: false                # default false: whole pipeline skipped
  # base GA (serve):
  # population_size: 20
  # elite_count: 2
  # survival_rate: 0.6          # [0,1]
  # mutation_rate: 0.2
  # min_mutation_rate: 0.05
  # max_mutation_rate: 0.5
  # generations: 15             # 0 semantics see article 22; validation requires >=1
  # breeding_pool_ratio: 0.5
  # min_interval: "5m"          # Go duration
  # selection_strategy: "tournament"  # tournament | rank | roulette | sus | truncation | random
  # tournament_size: 3
  # crossover_type: "uniform"   # uniform | two_point | segment
  # target_fitness: 0
  # steady_state: false
  # steady_state_replace_rate: 0.3
  # llm_scoring: { enabled: false, seed: 0, max_calls_per_generation: 100 }
  # control plane (optional; 0/absent falls back to code defaults):
  # lifecycle: { fitness_window: 50, min_samples_before_judge: 10, cold_start_score: 0.5,
  #             watch_interval: "30s", min_active_duration: "90s", outcome_weight, dimension_eval_weight,
  #             workflow_weight, scheduler_weight, recovery_weight, blacklist_generations: 3 }
  # rollback: { enabled: true, degradation_threshold: 0.15, window_size: 5, min_samples: 3 }
  # shadow: { min_samples: 20, min_win_rate: 0.55, replay_window_span: "10m", replay_query_limit: 200 }
  # shadow_execution: { enabled: false, sample_size: 3 }
  # channel_feedback: { collab_enabled: false, collab_weight: 0.0, tool_enabled: false, tool_weight: 0.0 }
  # tool_projection: { enabled: false, interval: "10m", min_samples: 3 }
  # gates: { eval_min_score: 0.7, require_manual_approval: false, eval_suite: "" }
  # tool_pool: []              # tool-whitelist candidates
  # guardrails: { max_tools_enabled: 0, require_any_tool: false, known_tools: [] }
  # deployment: ...            # safe promotion pipeline (deployment.DeploymentConfig)
```

Validation (serve `validateEvolution`): `population_size>=2`, `elite_count in [0, pop)`, `survival_rate in (0,1]`, `mutation_rate in [0,1]`, `generations>=1`; when `tool_projection` is armed, `interval>0` and `min_samples>=0`.

> Honest note: GA-engine internals (fitness aggregation, dream cycle, gate semantics) sit outside `internal/ares_config`'s field surface; only the YAML-configurable face is listed here.

---

## 5. Agents / Kernel / Tools / Reflection

`configs/ares.yaml` examples:

```yaml
agents:
  # C1 flat peer structure (default, no leader): each entry is an equal peer
  peers:
    - id: coder
      capabilities: ["code", "refactor"]   # full declared capability set
      # priority: 1.0                      # scheduling priority, >=0
      # role: ""                           # agent profile id (W4)
      # max_tool_rounds: 0                 # tool-loop cap, default 5
    - id: reviewer
      capabilities: ["review", "audit"]
    - id: researcher
      capabilities: ["research"]
  # sub: [...]   # legacy leader/sub list, still accepted as a fallback

kernel:
  # loop_round_quanta: 1       # scheduler quanta per loop round
  # loop_max_iterations: 0     # round-clock cap, 0 = unlimited
  # policy: "taskfabric"       # taskfabric | legacy (default taskfabric)
  # lease_ttl: "5m"            # task-lease duration (e.g. "45s")

tools:
  builtin: true               # built-in tools (calculator, web_search, file, etc.)
  # mcp:                       # stdio MCP server command list
  #   - npx @modelcontextprotocol/server-filesystem ./data

reflection:
  enabled: false              # agent self-reflection toggle
```

Serve-side extras: `kernel.policy`/`lease_ttl`/`resources`/`max_restarts`/various interval + timeout fields; each `agents.sub` entry `{id, type, category, triggers, model, provider, dependencies, role, priority, max_tool_rounds, ...}`.

---

## 6. Storage / Embedding / Auth

```yaml
# --- serve-side StorageConfig ---
storage:
  enabled: true
  type: postgres               # postgres | sqlite
  host: localhost
  port: 5432
  username: ares
  password: ""                 # json:"-" prevents JSON serialization leak
  database: ares
  ssl_mode: disable
  pgvector:
    enabled: false
    dimension: 1536
    table_name: embeddings

# --- sdk-side DatabaseFileConfig (configs/ares.yaml uses the database key) ---
database:
  host: localhost
  port: 5432
  user: ares
  password: ""
  database: ares
  ssl_mode: disable            # disable | require | verify-full

embedding:
  service_url: "http://localhost:8000/v1/embeddings"  # OpenAI-compatible
  model: "text-embedding-ada-002"

# --- serve-side SecurityConfig (JWT/RBAC) ---
security:
  jwt_secret: ""               # prefer ARES_JWT_SECRET, don't commit YAML
  jwt_expiry: "24h"
  auth_enabled: false
```

> Important naming difference: **serve uses `storage`, sdk uses `database`** — `configs/ares.yaml` carries both (each quoted above under its key). Pick the one for your path.

---

## Full Example (all features, anonymous block from configs/ares.yaml)

```yaml
llm:
  provider: openai
  model: gpt-4o
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

## Config Sources & Precedence

Config may come from **multiple sources**, merged into `ares_config.Config` (serve) or `sdk.ConfigFile` (sdk):

serve: `Config` struct → `setDefaults()` → YAML → `LoadFromEnv()` (`SERVER_*`/`LLM_*`/`DB_*`/`ARES_*`) → programmatic Options. Later sources override earlier ones; zero-value fields fall back to component defaults.

sdk: `LoadConfigFile` reads YAML → `Validate` → `ToOptions` turns it into `sdk.Option`s; API keys fall back to env vars only when unset, via `resolveAPIKey`.

> Honest note: the old article's "twelve sources / three-tier precedence" table cannot be verified line-by-line against the code, so it is not reproduced here; the exact env-var names are in `internal/ares_config/config.go`'s `LoadFromEnv` (listed in article 22).

## Related Docs

- Config system deep dive: `docs/articles/en/22-config-system.md`
- Full examples: `examples/01-quickstart/`, `examples/12-yaml-driven-flags/`; minimum bootstrap: `configs/ares.minimal.yaml`