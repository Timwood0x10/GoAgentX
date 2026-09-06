# ares 架构拆解 (XXII)：配置系统——一个 YAML，十七个模块（0.3.x）

每个模块都需要配置。LLM 要 provider 和 model。Memory 要历史长度。Evolution 要种群规模。Storage 要 host 和 port。模块一多，配置文件就是一地鸡毛——除非有配置系统。

`internal/ares_config/config.go`（1,084 行）+ `config_defaults.go` + `config_validate.go` 就是这套系统，SDK 侧再由 `sdk/config.go`（430 行）桥接。

> 诚实校正：旧文写"`config.go` 844 行、`sdk/config.go` 165 行、Config 十二个 section"。实际 `wc -l` 是 config.go 1,084 行、sdk/config.go 430 行，顶层 `Config` 目前有 **17 个** section（见下）。

---

## 问题：一堆配置源

早期配置分散且缺校验。

| 模块 | 来源 | 格式 |
|------|------|------|
| LLM | 环境变量 | `LLM_MODEL=...` 等 |
| 部分组件 | 结构体字面量/flag | Go / 命令行 |
| 部分组件 | 独立 YAML | YAML |
| Storage | 环境变量 | `DB_HOST`/`DB_PORT`/`DB_USERNAME`… |

来源多、格式杂、校验零散。缺个环境变量或有人拼错一个 YAML 键，往往是运行时才炸。

**坦诚反思**：我们评估过 Viper。它功能强，但"自动魔法"（环境绑定、远程配置、文件监听）总是给人惊吓。最后回到 `gopkg.in/yaml.v3` + 显式加载。少一个魔法来源，就少一个"为什么没生效"的深夜调试。

---

## 设计：一个 Config，有类型，有校验

### 根配置

```go
// internal/ares_config/config.go（字段与 yaml 键）
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

一个结构体，**17 个** section，每个是有 `yaml` tag 的有类型结构体。缺省都走 `setDefaults()`，再走 `Validate()`。

### 带路径遍历保护的加载

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
        // 拒绝用 ".." 逃出 allowed 目录的路径
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

`SetAllowedConfigDir()` 限制配置文件的加载范围。校验用的是 **`filepath.Rel` 的 `".."` 前缀拒绝**（加上 `allowedConfigDirMu` 读写锁，让 `SetAllowedConfigDir` 能和并发 `Load`/热重载安全竞争）。

> 诚实校正：旧文讲"曾用 `filepath.Rel`，在 Windows 失败，所以改成 `strings.HasPrefix`"。**与当前代码相反**——现状正是 `filepath.Rel` + `".."` 前缀检查。我们以代码为准，不做超出代码的"历史叙述"。

### 有类型校验

`Validate()` 按 section 逐项校验，出错快速返回：

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

不是运行时 panic，也不是静默失败，而是可操作的错误信息。例如 LLM provider 非法：

```
invalid LLM provider: foo, must be 'openai', 'ollama', 'openrouter', or 'anthropic'
```

MCP server 校验里，`stdio` 必须给 `command`、`sse` 必须给 `url`，否则启动即报错而非带病运行。

### 环境变量层（LoadFromEnv）

既有 YAML，也保留了环境变量覆盖（YAML 之上、程序化 Options 之下），已核实的变量名：

`SERVER_HOST`、`SERVER_PORT`、`LLM_API_KEY`、`OPENROUTER_API_KEY`（备选，仅在 LLM_API_KEY 为空时生效）、`LLM_PROVIDER`、`LLM_BASE_URL`、`LLM_MODEL`；存储 `DB_HOST`/`DB_PORT`/`DB_USERNAME`/`DB_PASSWORD`/`DB_DATABASE`；安全 `ARES_JWT_SECRET`、`ARES_AUTH_ENABLED`。

---

## 零值哲学

ares 有个配置哲学：**零值意味着"用组件默认值"**。三个点：

1. 你只配想调的。
2. 默认值在组件/`setDefaults` 里，不在调用方。
3. 加新配置项不破坏现有配置。

对"默认开启"的开关，用 **`*bool`** 表达**三态**（`nil`=默认、`false`=显式关）。memory 就是个典型：

```go
// internal/ares_config/config.go
type MemoryConfig struct {
    Enabled          *bool         `yaml:"enabled"`            // nil/true = 默认开; false = 关
    SessionMemory    SessionConfig `yaml:"session"`
    UserProfile      ProfileConfig `yaml:"user_profile"`
    TaskDistillation DistillConfig `yaml:"task_distillation"`
    MaxHistory       int           `yaml:"max_history"`        // 默认 10
    EnableDistillation *bool       `yaml:"enable_distillation"`// nil=默认开
    DistillationThreshold int      `yaml:"distillation_threshold"` // 默认 3
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

**坦诚反思**：零值哲学有代价——分不清用户是"故意设 0"还是"没配"。`*bool` 把这个代价收窄到三元开关，但它引入了解引用成本。对数值字段，实践中"没设"和"0"往往指向同一件事：用默认值。`*int`（nil=未设、0=显式零）我们也考虑过，复杂度不值当。

### 蒸馏阈值：两个字段，别混

旧文把 `DistillConfig.Threshold` 和 `memory.distillation_threshold` 当成了同一个。实际有**两个**：

- `memory.task_distillation.threshold`（`DistillConfig.Threshold`，`yaml:"threshold"`）：事件订阅路径里事前累积触发蒸馏的轮数，`0` = 不过门（旧行为）。
- `memory.distillation_threshold`（`MemoryConfig.DistillationThreshold`，默认 `3`）：闭环 memory 的蒸馏节流，`0` = 不过门、每事件触发；负值被校验拒绝。

看门狗校验也分开处理：`DistillConfig.Threshold` 与 `MemoryConfig.DistillationThreshold` 各自非负校验。

---

## 关键默认值（setDefaults）

`config_defaults.go` 统一填默认，让"只给 LLM"的最小配置也能跑。已核实的默认值：

| section | 默认值 |
|---|---|
| server.host / port | `localhost` / `8080` |
| llm.provider / model | `ollama` / `gemma4` |
| llm.timeout / max_tokens | `60` / `4096` |
| llm.scorer_api_rate / burst | `10` / `20` |
| output.format | `simple` |
| storage.type / port | `postgres` / `5432` |
| storage.pgvector.dimension / table_name | `1536` / `embeddings` |
| memory.max_history / session.max_history | `10` / `50` |
| memory.enable_distillation | 默认开（`*bool` nil→true） |
| memory.distillation_threshold | `3` |
| memory.archive.dir / max_rounds | `.context/rounds` / `200` |
| evolution 全套 | population 20 · elite 2 · survival 0.6 · mutation 0.2 · min 0.05 · max 0.5 · generations 15 · breeding 0.5 · min_interval `5m` · selection `tournament` · tournament_size 3 · crossover `uniform`；llm_scoring.max_calls_per_generation 100 |
| tool 投影 worker | interval `10m` · min_samples `3` |

> `setDefaults` 里 `evolution.*` 有 `DefaultEvolution*` 系列导出常量——bootstrapping 会用它们区分"运维调过"与"setDefaults 填的"。还隐含一个说明：**配置层默认值与 GA 引擎默认值刻意不同**（如 EliteCount 2 vs 3、BreedingPoolRatio 0.5 vs 0.6），没调过的字段必须保留引擎值。

`NewMinimalConfig(baseURL, apiKey, model)` 也是这套默认的入口：apiKey 非空推断 openai、否则 ollama，并装配一组默认 sub-agent（coder-a / reviewer-1 / researcher-1）。它是零 YAML 也能启动的基础。

---

## SDK 配置层

`sdk/config.go`（430 行）把 raw YAML 桥到 SDK `Option`。`ConfigFile` 的 shape 比 serve 侧小：LLM / Database / Embedding / Memory / Knowledge / Tools(builtin+mcp) / Reflection / Evolution。

```go
// sdk/config.go（签名与核心字段）
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

`ToOptions()` 把 provider 转成对应 `WithOpenAI`/`WithOllama`/`WithAnthropic`/`WithOpenRouter`，默认模型各不同（ollama→`llama3.2`、openai→`gpt-4o-mini`、anthropic→`claude-3-haiku`、openrouter→`openai/gpt-4o-mini`）；`llm.max_prompt_length` 由一条内联 Option 桥进 `cfg.llmCfg.MaxPromptLength`（旧版这里静默丢字段，长 Agent 跑到 8192 就挂——代码注释原话）；Database 给了 host 才 `WithPostgres`；memory enabled 才 `WithMemoryConfig`/`WithDistillation`/`WithRAG`，否则 `WithoutMemory()`；knowledge 有 chunk_size 才生效；evolution.enabled 才 `WithEvolution()`。API key 用 `resolveAPIKey(configKey, envVar)` 兜底环境变量。

用户于是可以：

```go
cfg, _ := ares.LoadConfigFile("ares.yaml")
opts, _ := cfg.ToOptions()
rt := ares.MustNew(opts...)
```

一个 YAML 文件驱动整个 SDK。

---

## 技能源配置（skill_sources，~/.ares/config.toml）

Capability Fabric 的注册源声明在 **`~/.ares/config.toml`**（不是 `ares.yaml`），由 `internal/ares_skills` 的 `LoadSkillSources`/`LoadRegisteredSkillDirs` 解析（`~` 展开 + 去重 + 未知类型跳过留作扩展点）：

```toml
[[skill_sources]]
type = "directory"          # 额外目录源
path = "~/my-company/ares-skills"

[[skill_sources]]
type = "git"                # git 源：浅克隆到本地缓存后索引
url = "https://example.com/skills.git"
local_dir = "~/.ares/cache/skills"

# type = "http" / "oci"：ManifestURL 字段已在结构里占位，
# 但代码注释标注 http/oci 为未来能力（待核实当前是否启用）
# [[skill_sources]]
# type = "http"
# manifest_url = "https://example.com/manifest.json"
```

关键点：project（`.ares/skills`）与 user（`~/.ares/skills`）是约定目录，**无需配置**；此文件只声明额外源——尊重"只扫声明源，零全盘扫描"。与零值哲学一致：不配置就没有额外源。

对应结构（`internal/ares_skills/config.go`）：`SkillSourceEntry{Type, Path, URL, LocalDir, ManifestURL}`，用 `pelletier/go-toml/v2` 解析。项目级目录源的类型确认为 `directory` 与 `git`。

---

## 教训

配置是没人庆祝的层。你没法给投资人演示 `Config.Validate()`。但它是"在我机器上能跑"和"生产能用"的区别。

配置系统是新用户第一个碰的东西（通过 `ares.yaml`），也是最后一个想到的东西（直到出问题）。让它有类型、有校验、零值友好、路径安全，意味着用户花更少时间配置，更多时间构建。

**最好的配置系统是你忘记它存在的那个。** 你写 `ares.yaml`，它就能跑。