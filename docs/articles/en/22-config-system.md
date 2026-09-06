# ares Architecture Deep Dive (XXII): Config System — One YAML, Seventeen Sections (0.3.x)

Every module needs configuration. LLM needs provider and model. Memory needs history size. Evolution needs population size. Storage needs host and port. Too many modules, and configs scatter everywhere — unless you have a config system.

`internal/ares_config/config.go` (1,084 lines) + `config_defaults.go` + `config_validate.go` are that system, bridged on the SDK side by `sdk/config.go` (430 lines).

> Honesty correction: the old article claimed "config.go 844 lines, sdk/config.go 165 lines, Config with twelve sections." Actual `wc -l` is 1,084 for config.go and 430 for sdk/config.go, and the top-level `Config` currently has **17** sections (below).

---

## The Problem: Scattered Config Sources

Early config was fragmented and lightly validated.

| Module | Source | Format |
|--------|--------|--------|
| LLM | Environment variables | `LLM_MODEL=...` and friends |
| Some components | Struct literals / flags | Go / CLI |
| Some components | Separate YAML | YAML |
| Storage | Environment variables | `DB_HOST`/`DB_PORT`/`DB_USERNAME`… |

Many sources, mixed formats, sparse validation. A missing env var or a misspelled YAML key usually exploded at runtime.

**Honest reflection**: We evaluated Viper. It's powerful, but the "auto-magic" (env binding, remote config, file watching) kept surprising us. We went back to `gopkg.in/yaml.v3` + explicit loading. One fewer magic source means one fewer "why didn't it apply" debugging session at 3 AM.

---

## The Design: One Config, Typed, Validated

### The Root Config

```go
// internal/ares_config/config.go (fields and YAML keys)
type Config struct {
    Server     ServerConfig     `yaml:"server"`
    LLM        LLMConfig        `yaml:"llm"`
    Agents     AgentsConfig     `yaml:"agents"`
    Tools      ToolsConfig      `yaml:"tools"`
    Prompts    PromptsConfig    `yaml:"prompts"`
    Output     OutputConfig     `yaml:"output"`
    Validation ValidationConfig `yaml:"validation"`
    Workflow   WorkflowConfig   `yaml:"workflow"`
    Storage    StorageConfig    `yaml:"storage"`
    Memory     MemoryConfig     `yaml:"memory"`
    Knowledge  KnowledgeConfig  `yaml:"knowledge"`
    MCP        MCPConfig        `yaml:"mcp"`
    Evolution  EvolutionConfig  `yaml:"evolution"`
    Embedding  EmbeddingConfig  `yaml:"embedding"`
    Discovery  DiscoveryConfig  `yaml:"discovery"`
    Kernel     KernelConfig     `yaml:"kernel"`
    Security   SecurityConfig   `yaml:"security"`
}
```

One struct, **17** sections, each a typed struct with `yaml` tags. Omitted fields get defaults from `setDefaults()`, then pass through `Validate()`.

### Loading with Path-Traversal Protection

```go
// internal/ares_config/config.go
func Load(path string) (*Config, error) {
    allowedConfigDirMu.RLock()
    dir := allowedConfigDir
    allowedConfigDirMu.RUnlock()
    if dir != "" {
        absPath, _ := filepath.Abs(path)
        absDir, _ := filepath.Abs(dir)
        rel, err := filepath.Rel(absDir, absPath)
        if err != nil { /* ... */ }
        // reject paths that escape the allowed directory via ".."
        if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
            return nil, fmt.Errorf("config path %s is outside allowed directory %s", path, dir)
        }
    }
    data, err := os.ReadFile(path)
    // ...
    var cfg Config
    yaml.Unmarshal(data, &cfg)
    cfg.setDefaults()
    cfg.Validate()
    return &cfg, nil
}
```

`SetAllowedConfigDir()` restricts where config files load from. The check is a **`filepath.Rel` + `".."`-prefix rejection** (guarded by `allowedConfigDirMu`, so `SetAllowedConfigDir` races safely with concurrent `Load`/hot-reload watchers).

> Honesty correction: the old article claimed "we used `filepath.Rel`, it failed on Windows, so we switched to `strings.HasPrefix`." **That contradicts the code** — the current implementation is `filepath.Rel` plus a `".."` prefix check. We report the code as it is and avoid inventing a history the source doesn't support.

### Typed Validation

`Validate()` walks section validators and fails fast:

```go
// internal/ares_config/config_validate.go
func (c *Config) Validate() error {
    if err := c.validateServer(); err != nil { return err }
    if err := c.validateLLM(); err != nil { return err }
    if err := c.validateAgents(); err != nil { return err }
    if err := c.validateOutput(); err != nil { return err }
    if err := c.validateStorage(); err != nil { return err }
    if err := c.validateMemory(); err != nil { return err }
    if err := c.validateKnowledge(); err != nil { return err }
    if err := c.validateMCP(); err != nil { return err }
    if err := c.validateEvolution(); err != nil { return err }
    if err := c.validateDiscovery(); err != nil { return err }
    if err := c.validateKernel(); err != nil { return err }
    return nil
}
```

Not a runtime panic, not a silent failure — an actionable error. An invalid LLM provider:

```
invalid LLM provider: foo, must be 'openai', 'ollama', 'openrouter', or 'anthropic'
```

MCP validation requires `stdio` to carry a `command` and `sse` to carry a `url`, refusing to boot with a broken transport rather than limping along.

### The Env-Var Layer (LoadFromEnv)

YAML stays, env-vars override above it (and below programmatic Options). Verified variable names:

`SERVER_HOST`, `SERVER_PORT`, `LLM_API_KEY`, `OPENROUTER_API_KEY` (fallback, only when `LLM_API_KEY` is empty), `LLM_PROVIDER`, `LLM_BASE_URL`, `LLM_MODEL`; storage `DB_HOST`/`DB_PORT`/`DB_USERNAME`/`DB_PASSWORD`/`DB_DATABASE`; security `ARES_JWT_SECRET`, `ARES_AUTH_ENABLED`.

---

## The Zero-Value Philosophy

ares has a config philosophy: **zero means "use the component default."** Three consequences:

1. You only configure what you want to tune.
2. Defaults live in `setDefaults`/components, not callers.
3. Adding a new option doesn't break existing configs.

For "default-on" switches, a **`*bool`** expresses a **tri-state** (`nil`=default, `false`=explicit off). Memory is the canonical example:

```go
// internal/ares_config/config.go
type MemoryConfig struct {
    Enabled          *bool         `yaml:"enabled"`             // nil/true = on by default; false = off
    SessionMemory    SessionConfig `yaml:"session"`
    UserProfile      ProfileConfig `yaml:"user_profile"`
    TaskDistillation DistillConfig `yaml:"task_distillation"`
    MaxHistory       int           `yaml:"max_history"`         // default 10
    EnableDistillation *bool       `yaml:"enable_distillation"` // nil = on by default
    DistillationThreshold int      `yaml:"distillation_threshold"` // default 3
    EnableRAG        bool          `yaml:"enable_rag"`
    RAGTopK          int           `yaml:"rag_top_k"`
    RAGMinScore      float64       `yaml:"rag_min_score"`
    Archive          ArchiveConfig `yaml:"archive"`
}

func (m MemoryConfig) IsEnabled() bool { return m.Enabled == nil || *m.Enabled }
func (m *MemoryConfig) DistillationEnabled() bool {
    return m.EnableDistillation == nil || *m.EnableDistillation
}
```

**Honest reflection**: The zero-value philosophy has a cost — you can't tell whether a user set 0 on purpose or didn't configure it. `*bool` narrows that cost to tri-state switches but adds dereferencing. For numeric fields, "unset" and "0" effectively mean the same thing: use the default. We considered `*int` (nil=unset, 0=explicit zero); the added complexity wasn't worth it.

### The Distillation Threshold: Two Fields, Don't Mix

The old article treated `DistillConfig.Threshold` and `memory.distillation_threshold` as one. There are actually **two**:

- `memory.task_distillation.threshold` (`DistillConfig.Threshold`, `yaml:"threshold"`): rounds that accumulate before distillation fires in the event-subscription path; `0` = ungated (legacy).
- `memory.distillation_threshold` (`MemoryConfig.DistillationThreshold`, default `3`): the closed-loop distillation throttle; `0` = ungated, fire every event; negatives rejected.

Validation handles them separately, each with its own non-negative check (`validateMemory`).

---

## Key Defaults (setDefaults)

`config_defaults.go` fills defaults so a "LLM-only" minimal config can still run. Verified defaults:

| section | default |
|---|---|
| server.host / port | `localhost` / `8080` |
| llm.provider / model | `ollama` / `gemma4` |
| llm.timeout / max_tokens | `60` / `4096` |
| llm.scorer_api_rate / burst | `10` / `20` |
| output.format | `simple` |
| storage.type / port | `postgres` / `5432` |
| storage.pgvector.dimension / table_name | `1536` / `embeddings` |
| memory.max_history / session.max_history | `10` / `50` |
| memory.enable_distillation | on by default (`*bool` nil→true) |
| memory.distillation_threshold | `3` |
| memory.archive.dir / max_rounds | `.context/rounds` / `200` |
| evolution (whole set) | population 20 · elite 2 · survival 0.6 · mutation 0.2 · min 0.05 · max 0.5 · generations 15 · breeding 0.5 · min_interval `5m` · selection `tournament` · tournament_size 3 · crossover `uniform`; llm_scoring.max_calls_per_generation 100 |
| tool projection worker | interval `10m` · min_samples `3` |

> `setDefaults` exposes `DefaultEvolution*` exported constants — bootstrap uses them to tell "operator tuned this" apart from "setDefaults filled it in." Note the config-layer evolution defaults are deliberately different from the GA engine's own defaults (e.g. EliteCount 2 vs 3, BreedingPoolRatio 0.5 vs 0.6); fields the operator didn't tune must keep the engine values.

`NewMinimalConfig(baseURL, apiKey, model)` is the same default entry point: a non-empty apiKey infers openai, otherwise ollama, and it assembles a default sub-agent team (coder-a / reviewer-1 / researcher-1). It's what lets the runtime start with zero YAML.

---

## The SDK Config Layer

`sdk/config.go` (430 lines) bridges raw YAML to SDK `Option`s. `ConfigFile`'s shape is smaller than serve-side: LLM / Database / Embedding / Memory / Knowledge / Tools(builtin+mcp) / Reflection / Evolution.

```go
// sdk/config.go (signatures and core fields)
type ConfigFile struct {
    LLM       LLMFileConfig       `yaml:"llm"`
    Database  DatabaseFileConfig  `yaml:"database"`
    Embedding EmbeddingFileConfig `yaml:"embedding"`
    Memory    MemoryFileConfig    `yaml:"memory"`
    Knowledge KnowledgeFileConfig `yaml:"knowledge"`
    Tools     struct{ Builtin bool `yaml:"builtin"`; MCP []string `yaml:"mcp"` } `yaml:"tools"`
    Reflection struct{ Enabled bool `yaml:"enabled"` } `yaml:"reflection"`
    Evolution struct{ Enabled bool `yaml:"enabled"` } `yaml:"evolution"`
}

func LoadConfigFile(path string) (*ConfigFile, error)
func (c *ConfigFile) Validate() error
func (c *ConfigFile) ToOptions() ([]Option, error)
```

`ToOptions()` maps each provider to `WithOpenAI`/`WithOllama`/`WithAnthropic`/`WithOpenRouter` with distinct default models (ollama→`llama3.2`, openai→`gpt-4o-mini`, anthropic→`claude-3-haiku`, openrouter→`openai/gpt-4o-mini`); `llm.max_prompt_length` is bridged into `cfg.llmCfg.MaxPromptLength` via an inline Option (the old code silently dropped the field, so long agent runs died at the 8192 provider default — the source comment's own words); Database only calls `WithPostgres` when host is set; memory enabled → `WithMemoryConfig`/`WithDistillation`/`WithRAG`, else `WithoutMemory()`; knowledge requires `chunk_size`; evolution only `WithEvolution()` when enabled. API keys fall back to env vars via `resolveAPIKey(configKey, envVar)`.

That lets users write:

```go
cfg, _ := ares.LoadConfigFile("ares.yaml")
opts, _ := cfg.ToOptions()
rt := ares.MustNew(opts...)
```

One YAML file drives the entire SDK.

---

## Skill Source Configuration (skill_sources, ~/.ares/config.toml)

The Capability Fabric's registered sources are declared in **`~/.ares/config.toml`** (not `ares.yaml`), parsed by `internal/ares_skills`'s `LoadSkillSources`/`LoadRegisteredSkillDirs` (`~` expansion + dedup + unknown types skipped as extension points):

```toml
[[skill_sources]]
type = "directory"          # extra directory source
path = "~/my-company/ares-skills"

[[skill_sources]]
type = "git"                # git source: shallow-cloned into a local cache, then indexed
url = "https://example.com/skills.git"
local_dir = "~/.ares/cache/skills"

# type = "http" / "oci": ManifestURL is field-reserved, but the source comment
# marks http/oci as a future capability (待核实 whether currently active)
# [[skill_sources]]
# type = "http"
# manifest_url = "https://example.com/manifest.json"
```

Key points: project (`.ares/skills`) and user (`~/.ares/skills`) sources are conventional directories that need **no configuration**; this file only declares extra sources — honoring "only declared sources are scanned, zero full-disk scanning." Consistent with the zero-value philosophy: no configuration means no extra sources.

The backing struct (`internal/ares_skills/config.go`): `SkillSourceEntry{Type, Path, URL, LocalDir, ManifestURL}`, parsed with `pelletier/go-toml/v2`. Confirmed directory types are `directory` and `git`.

---

## Lessons

Configuration is a layer nobody celebrates. You can't demo `Config.Validate()` to investors. But it's the difference between "works on my machine" and "works in production."

The config system is the first thing new users touch (via `ares.yaml`) and the last thing they think about (until something breaks). Making it typed, validated, zero-value-friendly, and path-safe means users spend less time configuring and more time building.

**The best config system is the one you forget exists.** You write `ares.yaml`, it just works.