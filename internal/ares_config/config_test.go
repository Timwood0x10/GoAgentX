// nolint: errcheck // Test code may ignore return values
package ares_config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLoad tests the Load function.
func TestLoad(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	configContent := `
server:
  host: "localhost"
  port: 8080

llm:
  provider: "ollama"
  model: "llama3.2"
  timeout: 60
  max_tokens: 4096

agents:
  sub: []

prompts:
  profile_extraction: "Extract profile from: {{.input}}"
  recommendation: "Recommend items for: {{.input}}"
  style_analysis: "Analyze style of: {{.input}}"

output:
  format: "simple"
  item_template: "{{.ItemID}}: {{.Name}}"
  summary_template: "Got {{.Count}} items"

validation:
  enabled: true
  schema_type: "default"
  retry_on_fail: true
  max_retries: 3
  strict_mode: false

workflow:
  definition_path: "./workflows"
  auto_reload: true
  reload_interval: 30

storage:
  enabled: true
  type: "postgres"
  host: "localhost"
  port: 5432
  username: "postgres"
  password: "postgres"
  database: "ARES"
  ssl_mode: "disable"
  pgvector:
    enabled: true
    dimension: 1536
    table_name: "embeddings"

memory:
  enabled: true
  session:
    enabled: true
    max_history: 50
  user_profile:
    enabled: true
    storage: "memory"
    vector_db: false
  task_distillation:
    enabled: true
    storage: "memory"
    vector_store: false
    prompt: "Summarize task"
`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	// Test loading valid config
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify loaded values
	if cfg.Server.Host != "localhost" {
		t.Errorf("Server.Host = %v, want localhost", cfg.Server.Host)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("Server.Port = %v, want 8080", cfg.Server.Port)
	}
	if cfg.LLM.Provider != defaultLLMProvider {
		t.Errorf("LLM.Provider = %v, want ollama", cfg.LLM.Provider)
	}
	if cfg.LLM.Model != "llama3.2" {
		t.Errorf("LLM.Model = %v, want llama3.2", cfg.LLM.Model)
	}
	if cfg.Output.Format != "simple" {
		t.Errorf("Output.Format = %v, want simple", cfg.Output.Format)
	}
	if cfg.Storage.Enabled != true {
		t.Errorf("Storage.Enabled = %v, want true", cfg.Storage.Enabled)
	}
	if !cfg.Memory.IsEnabled() {
		t.Errorf("Memory.IsEnabled() = %v, want true", cfg.Memory.IsEnabled())
	}
}

// TestLoadInvalidFile tests loading a non-existent file.
func TestLoadInvalidFile(t *testing.T) {
	_, err := Load("/nonexistent/config.yaml")
	if err == nil {
		t.Error("Load() expected error for non-existent file, got nil")
	}
}

// TestLoad_ToolPoolAndGuardrails parses the evolution.tool_pool and
// evolution.guardrails YAML blocks. These are the C6/KnownTools single-source
// configuration: the yaml enumerates the registered tool vocabulary and the
// tool-whitelist pool, so the mutator and the guardrail agree on what a valid
// whitelist looks like.
func TestLoad_ToolPoolAndGuardrails(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	configContent := `
server:
  host: "localhost"
llm:
  provider: "ollama"
  model: "llama3.2"
agents:
  sub: []
evolution:
  tool_pool:
    - "web_search,calculator"
    - "web_search,calculator,code_runner"
  guardrails:
    max_tools_enabled: 4
    require_any_tool: true
    known_tools:
      - "web_search"
      - "calculator"
      - "code_runner"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Evolution.ToolPool) != 2 {
		t.Fatalf("ToolPool len = %d, want 2: %v", len(cfg.Evolution.ToolPool), cfg.Evolution.ToolPool)
	}
	if cfg.Evolution.ToolPool[0] != "web_search,calculator" {
		t.Errorf("ToolPool[0] = %q, want %q", cfg.Evolution.ToolPool[0], "web_search,calculator")
	}
	g := cfg.Evolution.Guardrails
	if g.MaxToolsEnabled != 4 {
		t.Errorf("MaxToolsEnabled = %d, want 4", g.MaxToolsEnabled)
	}
	if !g.RequireAnyTool {
		t.Error("RequireAnyTool = false, want true")
	}
	if len(g.KnownTools) != 3 || g.KnownTools[0] != "web_search" {
		t.Errorf("KnownTools = %v, want [web_search calculator code_runner]", g.KnownTools)
	}
}

// TestLoad_GuardrailsAbsentDefaultsZero asserts an absent `guardrails` block
// yields zero-values (bound disabled, vocabulary disabled) — preserving the
// pre-existing permissive selection behavior.
func TestLoad_GuardrailsAbsentDefaultsZero(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	configContent := `
server:
  host: "localhost"
llm:
  provider: "ollama"
  model: "llama3.2"
agents:
  sub: []
evolution:
  enabled: true
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	g := cfg.Evolution.Guardrails
	if g.MaxToolsEnabled != 0 || g.RequireAnyTool || len(g.KnownTools) != 0 {
		t.Errorf("absent guardrails must default zero, got MaxToolsEnabled=%d RequireAnyTool=%v KnownTools=%v",
			g.MaxToolsEnabled, g.RequireAnyTool, g.KnownTools)
	}
}

// TestLoadInvalidYAML tests loading invalid YAML.
func TestLoadInvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.yaml")
	configContent := `
server:
  host: "localhost"
  port: invalid
`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	_, err := Load(configPath)
	if err == nil {
		t.Error("Load() expected error for invalid YAML, got nil")
	}
}

// TestLoadFromEnv tests loading configuration from environment variables.
//
//nolint:gocyclo // Test function with comprehensive test cases
func TestLoadFromEnv(t *testing.T) {
	// Create minimal config
	cfg := &Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
		LLM: LLMConfig{
			Provider: defaultLLMProvider,
			Model:    "llama3",
		},
		Storage: StorageConfig{
			Type: "postgres",
		},
	}

	// Set environment variables
	// Test code: os.Setenv is used to set environment variables for testing
	// nolint: errcheck // This is intentional in test code
	if err := os.Setenv("SERVER_HOST", "0.0.0.0"); err != nil {
		t.Fatalf("Failed to set SERVER_HOST: %v", err)
	}

	// Test code: os.Setenv is used to set environment variables for testing
	// nolint: errcheck // This is intentional in test code
	if err := os.Setenv("SERVER_PORT", "9000"); err != nil {
		t.Fatalf("Failed to set SERVER_PORT: %v", err)
	}
	if err := os.Setenv("LLM_API_KEY", "test-api-key"); err != nil {
		t.Fatalf("Failed to set LLM_API_KEY: %v", err)
	}
	if err := os.Setenv("LLM_PROVIDER", providerOpenAI); err != nil {
		t.Fatalf("Failed to set LLM_PROVIDER: %v", err)
	}
	if err := os.Setenv("LLM_BASE_URL", "https://api.openai.com"); err != nil {
		t.Fatalf("Failed to set LLM_BASE_URL: %v", err)
	}
	if err := os.Setenv("LLM_MODEL", "gpt-4"); err != nil {
		t.Fatalf("Failed to set LLM_MODEL: %v", err)
	}
	if err := os.Setenv("DB_HOST", "db.example.com"); err != nil {
		t.Fatalf("Failed to set DB_HOST: %v", err)
	}
	if err := os.Setenv("DB_PORT", "5433"); err != nil {
		t.Fatalf("Failed to set DB_PORT: %v", err)
	}
	if err := os.Setenv("DB_USERNAME", "user"); err != nil {
		t.Fatalf("Failed to set DB_USERNAME: %v", err)
	}
	if err := os.Setenv("DB_PASSWORD", "pass"); err != nil {
		t.Fatalf("Failed to set DB_PASSWORD: %v", err)
	}
	if err := os.Setenv("DB_DATABASE", "testdb"); err != nil {
		t.Fatalf("Failed to set DB_DATABASE: %v", err)
	}
	defer func() {
		// Test code: os.Unsetenv is used to clean up environment variables in test
		// nolint: errcheck // This is intentional in test code
		if err := os.Unsetenv("SERVER_HOST"); err != nil {
			t.Fatalf("Failed to unset SERVER_HOST: %v", err)
		}
		if err := os.Unsetenv("SERVER_PORT"); err != nil {
			t.Fatalf("Failed to unset SERVER_PORT: %v", err)
		}
		if err := os.Unsetenv("LLM_API_KEY"); err != nil {
			t.Fatalf("Failed to unset LLM_API_KEY: %v", err)
		}
		if err := os.Unsetenv("LLM_PROVIDER"); err != nil {
			t.Fatalf("Failed to unset LLM_PROVIDER: %v", err)
		}
		if err := os.Unsetenv("LLM_BASE_URL"); err != nil {
			t.Fatalf("Failed to unset LLM_BASE_URL: %v", err)
		}
		if err := os.Unsetenv("LLM_MODEL"); err != nil {
			t.Fatalf("Failed to unset LLM_MODEL: %v", err)
		}
		if err := os.Unsetenv("DB_HOST"); err != nil {
			t.Fatalf("Failed to unset DB_HOST: %v", err)
		}
		if err := os.Unsetenv("DB_PORT"); err != nil {
			t.Fatalf("Failed to unset DB_PORT: %v", err)
		}
		if err := os.Unsetenv("DB_USERNAME"); err != nil {
			t.Fatalf("Failed to unset DB_USERNAME: %v", err)
		}
		if err := os.Unsetenv("DB_PASSWORD"); err != nil {
			t.Fatalf("Failed to unset DB_PASSWORD: %v", err)
		}
		if err := os.Unsetenv("DB_DATABASE"); err != nil {
			t.Fatalf("Failed to unset DB_DATABASE: %v", err)
		}
	}()

	// Load from environment
	if err := LoadFromEnv(cfg); err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	// Verify environment overrides
	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("Server.Host = %v, want 0.0.0.0", cfg.Server.Host)
	}
	if cfg.Server.Port != 9000 {
		t.Errorf("Server.Port = %v, want 9000", cfg.Server.Port)
	}
	if cfg.LLM.APIKey != "test-api-key" {
		t.Errorf("LLM.APIKey = %v, want test-api-key", cfg.LLM.APIKey)
	}
	if cfg.LLM.Provider != providerOpenAI {
		t.Errorf("LLM.Provider = %v, want openai", cfg.LLM.Provider)
	}
	if cfg.LLM.BaseURL != "https://api.openai.com" {
		t.Errorf("LLM.BaseURL = %v, want https://api.openai.com", cfg.LLM.BaseURL)
	}
	if cfg.LLM.Model != "gpt-4" {
		t.Errorf("LLM.Model = %v, want gpt-4", cfg.LLM.Model)
	}
	if cfg.Storage.Host != "db.example.com" {
		t.Errorf("Storage.Host = %v, want db.example.com", cfg.Storage.Host)
	}
	if cfg.Storage.Port != 5433 {
		t.Errorf("Storage.Port = %v, want 5433", cfg.Storage.Port)
	}
	if cfg.Storage.Username != "user" {
		t.Errorf("Storage.Username = %v, want user", cfg.Storage.Username)
	}
	if cfg.Storage.Password != "pass" {
		t.Errorf("Storage.Password = %v, want pass", cfg.Storage.Password)
	}
	if cfg.Storage.Database != "testdb" {
		t.Errorf("Storage.Database = %v, want testdb", cfg.Storage.Database)
	}
}

// TestLoadFromEnvOpenRouterAPIKey tests OPENROUTER_API_KEY environment variable.
func TestLoadFromEnvOpenRouterAPIKey(t *testing.T) {
	cfg := &Config{
		LLM: LLMConfig{
			Provider: providerOpenRouter,
		},
	}

	if err := os.Setenv("OPENROUTER_API_KEY", "openrouter-key"); err != nil {
		t.Fatalf("Failed to set OPENROUTER_API_KEY: %v", err)
	}
	defer func() {
		if err := os.Unsetenv("OPENROUTER_API_KEY"); err != nil {
			t.Fatalf("Failed to unset OPENROUTER_API_KEY: %v", err)
		}
	}()

	if err := LoadFromEnv(cfg); err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if cfg.LLM.APIKey != "openrouter-key" {
		t.Errorf("LLM.APIKey = %v, want openrouter-key", cfg.LLM.APIKey)
	}
}

// TestLoadFromEnvInvalidPort tests loading invalid port from environment.
func TestLoadFromEnvInvalidPort(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
	}

	if err := os.Setenv("SERVER_PORT", "invalid"); err != nil {
		t.Fatalf("Failed to set SERVER_PORT: %v", err)
	}
	defer func() {
		if err := os.Unsetenv("SERVER_PORT"); err != nil {
			t.Logf("Failed to unset SERVER_PORT: %v", err)
		}
	}()

	// Should not fail, just ignore invalid value
	if err := LoadFromEnv(cfg); err != nil {
		t.Errorf("LoadFromEnv() should ignore invalid port, got error: %v", err)
	}

	// Port should remain unchanged
	if cfg.Server.Port != 8080 {
		t.Errorf("Server.Port = %v, want 8080 (unchanged)", cfg.Server.Port)
	}
}

// TestLoadFromEnvSecurity verifies the JWT/security environment variables are
// loaded: ARES_JWT_SECRET sets Security.JWTSecret and ARES_AUTH_ENABLED turns
// on Security.AuthEnabled.
func TestLoadFromEnvSecurity(t *testing.T) {
	cfg := &Config{}
	if err := os.Setenv("ARES_JWT_SECRET", "env-secret"); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	if err := os.Setenv("ARES_AUTH_ENABLED", "1"); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	defer func() {
		_ = os.Unsetenv("ARES_JWT_SECRET")
		_ = os.Unsetenv("ARES_AUTH_ENABLED")
	}()

	if err := LoadFromEnv(cfg); err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.Security.JWTSecret != "env-secret" {
		t.Errorf("Security.JWTSecret = %q, want env-secret", cfg.Security.JWTSecret)
	}
	if !cfg.Security.AuthEnabled {
		t.Error("Security.AuthEnabled = false, want true")
	}
}

// TestSetDefaults tests the setDefaults method.
func TestSetDefaults(t *testing.T) {
	cfg := &Config{}

	cfg.setDefaults()

	// Verify default values
	if cfg.Server.Host != "localhost" {
		t.Errorf("Server.Host default = %v, want localhost", cfg.Server.Host)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("Server.Port default = %v, want 8080", cfg.Server.Port)
	}
	if cfg.LLM.Provider != defaultLLMProvider {
		t.Errorf("LLM.Provider default = %v, want ollama", cfg.LLM.Provider)
	}
	if cfg.LLM.Model != defaultLLMModel {
		t.Errorf("LLM.Model default = %v, want %s", cfg.LLM.Model, defaultLLMModel)
	}
	if cfg.LLM.Timeout != 60 {
		t.Errorf("LLM.Timeout default = %v, want 60", cfg.LLM.Timeout)
	}
	if cfg.LLM.MaxTokens != 4096 {
		t.Errorf("LLM.MaxTokens default = %v, want 4096", cfg.LLM.MaxTokens)
	}
	if cfg.Output.Format != "simple" {
		t.Errorf("Output.Format default = %v, want simple", cfg.Output.Format)
	}
	if cfg.Storage.Type != "postgres" {
		t.Errorf("Storage.Type default = %v, want postgres", cfg.Storage.Type)
	}
	if cfg.Storage.Port != 5432 {
		t.Errorf("Storage.Port default = %v, want 5432", cfg.Storage.Port)
	}
	if cfg.Storage.PGVector.Dimension != 1536 {
		t.Errorf("Storage.PGVector.Dimension default = %v, want 1536", cfg.Storage.PGVector.Dimension)
	}
	if cfg.Storage.PGVector.TableName != "embeddings" {
		t.Errorf("Storage.PGVector.TableName default = %v, want embeddings", cfg.Storage.PGVector.TableName)
	}
	if cfg.Memory.SessionMemory.MaxHistory != 50 {
		t.Errorf("Memory.SessionMemory.MaxHistory default = %v, want 50", cfg.Memory.SessionMemory.MaxHistory)
	}
	if cfg.Memory.UserProfile.Storage != "memory" {
		t.Errorf("Memory.UserProfile.Storage default = %v, want memory", cfg.Memory.UserProfile.Storage)
	}
	if cfg.Validation.SchemaType != "default" {
		t.Errorf("Validation.SchemaType default = %v, want default", cfg.Validation.SchemaType)
	}
	if cfg.Validation.MaxRetries != 3 {
		t.Errorf("Validation.MaxRetries default = %v, want 3", cfg.Validation.MaxRetries)
	}
	// Prompt templates must default so a config that omits the prompts
	// section still renders a meaningful worker prompt (previously an empty
	// prompts.recommendation rendered an empty prompt → provider 400 on
	// empty user content → 20s failover cooldown per worker call).
	if cfg.Prompts.Recommendation == "" {
		t.Error("Prompts.Recommendation default = empty, want DefaultRecommendationPrompt")
	}
	if cfg.Prompts.ProfileExtraction == "" {
		t.Error("Prompts.ProfileExtraction default = empty, want DefaultProfileExtractionPrompt")
	}
	if cfg.Prompts.StyleAnalysis == "" {
		t.Error("Prompts.StyleAnalysis default = empty, want DefaultStyleAnalysisPrompt")
	}
	if cfg.Prompts.Recommendation != DefaultRecommendationPrompt {
		t.Errorf("Prompts.Recommendation default = %q, want DefaultRecommendationPrompt", cfg.Prompts.Recommendation)
	}
}

// TestValidate tests the Validate method with valid config.
func TestValidate(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
		LLM: LLMConfig{
			Provider:  defaultLLMProvider,
			Model:     "llama3",
			Timeout:   60,
			MaxTokens: 4096,
		},
		Agents: AgentsConfig{
			Sub: []SubAgentConfig{
				{
					ID:         "sub-1",
					Type:       "top",
					Category:   "clothing",
					Timeout:    30,
					MaxRetries: 3,
				},
			},
		},
		Output: OutputConfig{
			Format: "simple",
		},
		Validation: ValidationConfig{
			MaxRetries: 3,
		},
		Storage: StorageConfig{
			Enabled:  true,
			Type:     "postgres",
			Host:     "localhost",
			Port:     5432,
			Database: "ARES",
		},
		Memory: MemoryConfig{
			SessionMemory: SessionConfig{
				MaxHistory: 50,
			},
			Archive: ArchiveConfig{Dir: ".context/rounds", MaxRounds: 200},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() error = %v", err)
	}
}

// TestValidateInvalidServerPort tests validation with invalid server port.
func TestValidateInvalidServerPort(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: 70000, // Invalid port
		},
		LLM: LLMConfig{
			Provider:  defaultLLMProvider,
			Model:     "llama3",
			Timeout:   60,
			MaxTokens: 4096,
		},
		Agents: AgentsConfig{
			Sub: []SubAgentConfig{},
		},
		Output: OutputConfig{
			Format: "simple",
		},
		Validation: ValidationConfig{
			MaxRetries: 3,
		},
		Memory: MemoryConfig{
			SessionMemory: SessionConfig{
				MaxHistory: 50,
			},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for invalid server port, got nil")
	}
}

// TestValidateInvalidLLMTimeout tests validation with invalid LLM timeout.
func TestValidateInvalidLLMTimeout(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
		LLM: LLMConfig{
			Provider:  defaultLLMProvider,
			Model:     "llama3",
			Timeout:   0, // Invalid timeout
			MaxTokens: 4096,
		},
		Agents: AgentsConfig{
			Sub: []SubAgentConfig{},
		},
		Output: OutputConfig{
			Format: "simple",
		},
		Validation: ValidationConfig{
			MaxRetries: 3,
		},
		Memory: MemoryConfig{
			SessionMemory: SessionConfig{
				MaxHistory: 50,
			},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for invalid LLM timeout, got nil")
	}
}

// TestValidateInvalidLLMProvider tests validation with invalid LLM provider.
func TestValidateInvalidLLMProvider(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
		LLM: LLMConfig{
			Provider:  "invalid", // Invalid provider
			Model:     "llama3",
			Timeout:   60,
			MaxTokens: 4096,
		},
		Agents: AgentsConfig{
			Sub: []SubAgentConfig{},
		},
		Output: OutputConfig{
			Format: "simple",
		},
		Validation: ValidationConfig{
			MaxRetries: 3,
		},
		Memory: MemoryConfig{
			SessionMemory: SessionConfig{
				MaxHistory: 50,
			},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for invalid LLM provider, got nil")
	}
}

// TestValidateInvalidOutputFormat tests validation with invalid output format.
func TestValidateInvalidOutputFormat(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
		LLM: LLMConfig{
			Provider:  defaultLLMProvider,
			Model:     "llama3",
			Timeout:   60,
			MaxTokens: 4096,
		},
		Agents: AgentsConfig{
			Sub: []SubAgentConfig{},
		},
		Output: OutputConfig{
			Format: "invalid", // Invalid format
		},
		Validation: ValidationConfig{
			MaxRetries: 3,
		},
		Memory: MemoryConfig{
			SessionMemory: SessionConfig{
				MaxHistory: 50,
			},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for invalid output format, got nil")
	}
}

// TestValidateInvalidSubAgent tests validation with invalid sub-agent config.
func TestValidateInvalidSubAgent(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
		LLM: LLMConfig{
			Provider:  defaultLLMProvider,
			Model:     "llama3",
			Timeout:   60,
			MaxTokens: 4096,
		},
		Agents: AgentsConfig{
			Sub: []SubAgentConfig{
				{
					ID:         "", // Invalid: empty ID
					Type:       "top",
					Category:   "clothing",
					Timeout:    30,
					MaxRetries: 3,
				},
			},
		},
		Output: OutputConfig{
			Format: "simple",
		},
		Validation: ValidationConfig{
			MaxRetries: 3,
		},
		Memory: MemoryConfig{
			SessionMemory: SessionConfig{
				MaxHistory: 50,
			},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for invalid sub-agent, got nil")
	}
}

// TestValidateStorageEnabled tests validation with storage enabled but missing required fields.
func TestValidateStorageEnabled(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
		LLM: LLMConfig{
			Provider:  defaultLLMProvider,
			Model:     "llama3",
			Timeout:   60,
			MaxTokens: 4096,
		},
		Agents: AgentsConfig{
			Sub: []SubAgentConfig{},
		},
		Output: OutputConfig{
			Format: "simple",
		},
		Validation: ValidationConfig{
			MaxRetries: 3,
		},
		Storage: StorageConfig{
			Enabled:  true,
			Type:     "postgres",
			Host:     "", // Missing required field
			Port:     5432,
			Database: "ARES",
		},
		Memory: MemoryConfig{
			SessionMemory: SessionConfig{
				MaxHistory: 50,
			},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for storage enabled but missing host, got nil")
	}
}

// TestValidateInvalidSessionMaxHistory tests validation with invalid session max history.
func TestValidateInvalidSessionMaxHistory(t *testing.T) {
	cfg := &Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
		LLM: LLMConfig{
			Provider:  defaultLLMProvider,
			Model:     "llama3",
			Timeout:   60,
			MaxTokens: 4096,
		},
		Agents: AgentsConfig{
			Sub: []SubAgentConfig{},
		},
		Output: OutputConfig{
			Format: "simple",
		},
		Validation: ValidationConfig{
			MaxRetries: 3,
		},
		Memory: MemoryConfig{
			SessionMemory: SessionConfig{
				MaxHistory: -1, // Invalid
			},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("Validate() expected error for invalid session max history, got nil")
	}
}

// TestConfigStructs tests config struct initialization.
func TestConfigStructs(t *testing.T) {
	// Test ServerConfig
	serverCfg := ServerConfig{
		Host: "0.0.0.0",
		Port: 9000,
	}
	if serverCfg.Host != "0.0.0.0" || serverCfg.Port != 9000 {
		t.Error("ServerConfig initialization failed")
	}

	// Test LLMConfig
	llmCfg := LLMConfig{
		Provider:  providerOpenAI,
		APIKey:    "test-key",
		BaseURL:   "https://api.openai.com",
		Model:     "gpt-4",
		Timeout:   120,
		MaxTokens: 8192,
		Extra:     map[string]string{"custom": "value"},
	}
	if llmCfg.Provider != providerOpenAI || llmCfg.APIKey != "test-key" {
		t.Error("LLMConfig initialization failed")
	}

	// Test SubAgentConfig
	subCfg := SubAgentConfig{
		ID:         "sub-1",
		Type:       "top",
		Category:   "clothing",
		Triggers:   []string{"style", "budget"},
		MaxRetries: 3,
		Timeout:    30,
		Model:      "gpt-3.5",
		Provider:   providerOpenAI,
	}
	if subCfg.Type != "top" || len(subCfg.Triggers) != 2 {
		t.Error("SubAgentConfig initialization failed")
	}

	// Test StorageConfig
	storageCfg := StorageConfig{
		Enabled:  true,
		Type:     "postgres",
		Host:     "localhost",
		Port:     5432,
		Username: "postgres",
		Password: "postgres",
		Database: "ARES",
		SSLMode:  "disable",
		PGVector: PGVectorConfig{
			Enabled:   true,
			Dimension: 1536,
			TableName: "embeddings",
		},
	}
	if storageCfg.Type != "postgres" || storageCfg.Port != 5432 {
		t.Error("StorageConfig initialization failed")
	}

	// Test MemoryConfig
	memoryCfg := MemoryConfig{
		Enabled: boolPtr(true),
		SessionMemory: SessionConfig{
			Enabled:    true,
			MaxHistory: 100,
		},
		UserProfile: ProfileConfig{
			Enabled:  true,
			Storage:  "postgres",
			VectorDB: true,
		},
		TaskDistillation: DistillConfig{
			Enabled:     true,
			Storage:     "postgres",
			VectorStore: true,
			Prompt:      "Test prompt",
		},
	}
	if !memoryCfg.IsEnabled() || memoryCfg.SessionMemory.MaxHistory != 100 {
		t.Error("MemoryConfig initialization failed")
	}
}

// boolPtr returns a pointer to a bool literal for *bool config fields.
func boolPtr(b bool) *bool { return &b }

// TestValidLLMProviders tests all valid LLM providers.
func TestValidLLMProviders(t *testing.T) {
	validProviders := []string{providerOpenAI, defaultLLMProvider, providerOpenRouter}

	for _, provider := range validProviders {
		cfg := &Config{
			Server: ServerConfig{
				Host: "localhost",
				Port: 8080,
			},
			LLM: LLMConfig{
				Provider:  provider,
				Model:     "model",
				Timeout:   60,
				MaxTokens: 4096,
			},
			Agents: AgentsConfig{
				Sub: []SubAgentConfig{},
			},
			Output: OutputConfig{
				Format: "simple",
			},
			Validation: ValidationConfig{
				MaxRetries: 3,
			},
			Memory: MemoryConfig{
				SessionMemory: SessionConfig{
					MaxHistory: 50,
				},
				Archive: ArchiveConfig{Dir: ".context/rounds", MaxRounds: 200},
			},
		}

		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() failed for provider %s: %v", provider, err)
		}
	}
}

// TestValidOutputFormats tests all valid output formats.
func TestValidOutputFormats(t *testing.T) {
	validFormats := []string{"table", "json", "simple"}

	for _, format := range validFormats {
		cfg := &Config{
			Server: ServerConfig{
				Host: "localhost",
				Port: 8080,
			},
			LLM: LLMConfig{
				Provider:  defaultLLMProvider,
				Model:     "llama3",
				Timeout:   60,
				MaxTokens: 4096,
			},
			Agents: AgentsConfig{
				Sub: []SubAgentConfig{},
			},
			Output: OutputConfig{
				Format: format,
			},
			Validation: ValidationConfig{
				MaxRetries: 3,
			},
			Memory: MemoryConfig{
				SessionMemory: SessionConfig{
					MaxHistory: 50,
				},
				Archive: ArchiveConfig{Dir: ".context/rounds", MaxRounds: 200},
			},
		}

		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() failed for format %s: %v", format, err)
		}
	}
}

// nolint: errcheck // Test code may ignore return values

// TestSetAllowedConfigDir_PathTraversal verifies the path-traversal guard:
// configs inside the allowed directory load, configs that escape it via ".."
// are rejected before any file is read.
func TestSetAllowedConfigDir_PathTraversal(t *testing.T) {
	tmpDir := t.TempDir()
	innerDir := filepath.Join(tmpDir, "allowed")
	if err := os.MkdirAll(innerDir, 0755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", innerDir, err)
	}

	// A minimal valid config file.
	configContent := "server:\n  host: \"localhost\"\n  port: 8080\nllm:\n  provider: \"ollama\"\n  model: \"llama3.2\"\n"
	inConfig := filepath.Join(innerDir, "config.yaml")
	outConfig := filepath.Join(tmpDir, "secret.yaml")
	if err := os.WriteFile(inConfig, []byte(configContent), 0644); err != nil {
		t.Fatalf("WriteFile(%q): %v", inConfig, err)
	}
	if err := os.WriteFile(outConfig, []byte(configContent), 0644); err != nil {
		t.Fatalf("WriteFile(%q): %v", outConfig, err)
	}

	SetAllowedConfigDir(innerDir)
	t.Cleanup(func() { SetAllowedConfigDir("") })

	// Inside the allowed dir: loads fine.
	if _, err := Load(inConfig); err != nil {
		t.Errorf("Load(%q) inside allowed dir failed: %v", inConfig, err)
	}

	// Outside via "..": must be rejected.
	if _, err := Load(filepath.Join(innerDir, "..", "secret.yaml")); err == nil {
		t.Error("Load() outside allowed dir succeeded, want path-traversal rejection")
	}

	// Outside via absolute path: must be rejected.
	if _, err := Load(outConfig); err == nil {
		t.Error("Load() of absolute path outside allowed dir succeeded, want rejection")
	}

	// Resetting the guard (empty dir) restores unrestricted loading.
	SetAllowedConfigDir("")
	if _, err := Load(outConfig); err != nil {
		t.Errorf("Load(%q) after clearing allowed dir failed: %v", outConfig, err)
	}
}

// validKernelTestConfig returns a config that passes every validator, so a
// kernel-specific assertion cannot be satisfied by an unrelated earlier error
// (validateKernel runs last in Validate).
func validKernelTestConfig() *Config {
	return &Config{
		Server: ServerConfig{Host: "localhost", Port: 8080},
		LLM: LLMConfig{
			Provider:  defaultLLMProvider,
			Model:     "llama3",
			Timeout:   60,
			MaxTokens: 4096,
		},
		Output:     OutputConfig{Format: "simple"},
		Validation: ValidationConfig{MaxRetries: 3},
		Memory: MemoryConfig{
			SessionMemory: SessionConfig{MaxHistory: 50},
			Archive:       ArchiveConfig{Dir: ".context/rounds", MaxRounds: 200},
		},
	}
}

// TestValidateKernelLoopKnobs covers the loop-clock knobs' validation contract:
// zero/unset is legal (0 = unlimited rounds, 0 = default 1 quantum per round)
// because both are zero-value-safe, while negatives are rejected rather than
// silently normalized — a negative is always an operator mistake.
func TestValidateKernelLoopKnobs(t *testing.T) {
	t.Run("zero values are legal", func(t *testing.T) {
		cfg := validKernelTestConfig()
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() with unset kernel loop knobs error = %v, want nil", err)
		}
	})

	t.Run("negative loop_max_iterations rejected", func(t *testing.T) {
		cfg := validKernelTestConfig()
		cfg.Kernel.LoopMaxIterations = -1
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() with negative loop_max_iterations = nil, want error")
		}
		if !strings.Contains(err.Error(), "loop_max_iterations") {
			t.Errorf("error %q must name the offending key loop_max_iterations", err)
		}
	})

	t.Run("negative loop_round_quanta rejected", func(t *testing.T) {
		cfg := validKernelTestConfig()
		cfg.Kernel.LoopRoundQuanta = -3
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() with negative loop_round_quanta = nil, want error")
		}
		if !strings.Contains(err.Error(), "loop_round_quanta") {
			t.Errorf("error %q must name the offending key loop_round_quanta", err)
		}
	})

	t.Run("positive values pass through", func(t *testing.T) {
		cfg := validKernelTestConfig()
		cfg.Kernel.LoopMaxIterations = 10
		cfg.Kernel.LoopRoundQuanta = 4
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() with positive kernel loop knobs error = %v, want nil", err)
		}
	})
}

// TestValidateKernelDAGExecution covers the dag_execution knobs' validation
// contract (M4-A1/A2): an absent section is legal (legacy ReAct everywhere,
// planner default depth) because every field is zero-value-safe, while a
// negative max_plan_depth is rejected rather than silently normalized — a
// negative is always an operator mistake, and silently substituting a default
// would hide it.
func TestValidateKernelDAGExecution(t *testing.T) {
	t.Run("absent section is legal", func(t *testing.T) {
		cfg := validKernelTestConfig()
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() with absent dag_execution error = %v, want nil", err)
		}
		if cfg.Kernel.DAGExecution.Enabled {
			t.Error("absent dag_execution must default to disabled (legacy ReAct)")
		}
		if cfg.Kernel.DAGExecution.MaxPlanDepth != 0 {
			t.Errorf("absent max_plan_depth = %d, want 0 (planner default)",
				cfg.Kernel.DAGExecution.MaxPlanDepth)
		}
	})

	t.Run("negative max_plan_depth rejected", func(t *testing.T) {
		cfg := validKernelTestConfig()
		cfg.Kernel.DAGExecution.MaxPlanDepth = -1
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() with negative max_plan_depth = nil, want error")
		}
		if !strings.Contains(err.Error(), "max_plan_depth") {
			t.Errorf("error %q must name the offending key max_plan_depth", err)
		}
	})

	t.Run("negative reaper_grace rejected", func(t *testing.T) {
		cfg := validKernelTestConfig()
		cfg.Kernel.DAGExecution.ReaperGrace = -time.Second
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() with negative reaper_grace = nil, want error")
		}
		if !strings.Contains(err.Error(), "reaper_grace") {
			t.Errorf("error %q must name the offending key reaper_grace", err)
		}
	})

	t.Run("positive values pass through", func(t *testing.T) {
		cfg := validKernelTestConfig()
		cfg.Kernel.DAGExecution.Enabled = true
		cfg.Kernel.DAGExecution.MaxPlanDepth = 3
		cfg.Kernel.DAGExecution.ReaperGrace = 90 * time.Second
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() with positive dag_execution knobs error = %v, want nil", err)
		}
	})
}

// TestLoad_DAGExecutionSection verifies the kernel.dag_execution yaml keys
// parse into the gate config end to end, and that a file without the section
// loads with the gate off (shipped behavior unchanged).
func TestLoad_DAGExecutionSection(t *testing.T) {
	skeleton := `
server:
  host: "localhost"
  port: 8080

llm:
  provider: "ollama"
  model: "llama3.2"
  timeout: 60
  max_tokens: 4096

agents:
  sub: []
`
	t.Run("section present", func(t *testing.T) {
		configPath := filepath.Join(t.TempDir(), "config.yaml")
		content := skeleton + `
kernel:
  dag_execution:
    enabled: true
    max_plan_depth: 3
    reaper_grace: 45s
`
		if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write config file: %v", err)
		}
		cfg, err := Load(configPath)
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !cfg.Kernel.DAGExecution.Enabled {
			t.Error("dag_execution.enabled = false, want true")
		}
		if cfg.Kernel.DAGExecution.MaxPlanDepth != 3 {
			t.Errorf("dag_execution.max_plan_depth = %d, want 3",
				cfg.Kernel.DAGExecution.MaxPlanDepth)
		}
		if cfg.Kernel.DAGExecution.ReaperGrace != 45*time.Second {
			t.Errorf("dag_execution.reaper_grace = %s, want 45s",
				cfg.Kernel.DAGExecution.ReaperGrace)
		}
	})

	t.Run("section absent stays off", func(t *testing.T) {
		configPath := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(configPath, []byte(skeleton), 0644); err != nil {
			t.Fatalf("Failed to write config file: %v", err)
		}
		cfg, err := Load(configPath)
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.Kernel.DAGExecution.Enabled {
			t.Error("absent dag_execution must load as disabled")
		}
		if cfg.Kernel.DAGExecution.MaxPlanDepth != 0 {
			t.Errorf("absent max_plan_depth = %d, want 0",
				cfg.Kernel.DAGExecution.MaxPlanDepth)
		}
	})
}
