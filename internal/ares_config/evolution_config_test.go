// Package ares_config — evolution loop closure (E2) config tests.
//
// Verifies the EvolutionRollbackConfig tri-state semantics: the rollback safety
// net defaults ON (a nil Enabled pointer = armed, because the promote path
// relies on it) and can only be armed off by an explicit `enabled: false`. It
// also locks that an explicit `rollback.enabled: false` YAML file actually
// parses to IsEnabled()==false — the path the E2 plan claims was hard-coded to
// true in an earlier revision.
package ares_config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvolutionRollbackConfig_IsEnabled(t *testing.T) {
	t.Run("nil_pointer_is_enabled", func(t *testing.T) {
		cfg := EvolutionRollbackConfig{Enabled: nil}
		assert.True(t, cfg.IsEnabled(),
			"nil Enabled must default to armed — an operator who omits rollback gets the safety net")
	})

	t.Run("true_pointer_is_enabled", func(t *testing.T) {
		b := true
		cfg := EvolutionRollbackConfig{Enabled: &b}
		assert.True(t, cfg.IsEnabled())
	})

	t.Run("false_pointer_is_disabled", func(t *testing.T) {
		b := false
		cfg := EvolutionRollbackConfig{Enabled: &b}
		assert.False(t, cfg.IsEnabled(),
			"only an explicit enabled:false disarms the rollback net")
	})
}

// TestEvolutionRollbackConfig_YAMLDisabled honors the `rollback.enabled: false`
// YAML path — the case that was hard-coded to true before E2.
func TestEvolutionRollbackConfig_YAMLDisabled(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/evolution.yaml"
	content := "llm:\n  provider: ollama\nevolution:\n  enabled: true\n  rollback:\n    enabled: false\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)

	assert.True(t, cfg.Evolution.Enabled, "evolution.enabled: true must be honored")
	assert.False(t, cfg.Evolution.Rollback.IsEnabled(),
		"explicit rollback.enabled: false must disarm the rollback net")
}

// TestEvolutionRollbackConfig_YAMLDefaultOn covers the absent-block case: a
// YAML that mentions evolution but never mentions rollback still arms the net.
func TestEvolutionRollbackConfig_YAMLDefaultOn(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/evolution_defaulton.yaml"
	content := "llm:\n  provider: ollama\nevolution:\n  enabled: true\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)

	assert.True(t, cfg.Evolution.Enabled)
	assert.True(t, cfg.Evolution.Rollback.IsEnabled(),
		"absent rollback block must default the net to armed")
}

// TestEvolutionRollbackConfig_YAMLMinActiveDuration locks the promote-throttle
// knob parses through to the config struct (the raw string; the duration is
// validated at the bootstrap mapping layer).
func TestEvolutionRollbackConfig_YAMLMinActiveDuration(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/evolution_throttle.yaml"
	content := "llm:\n  provider: ollama\nevolution:\n  lifecycle:\n    min_active_duration: 90s\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, "90s", cfg.Evolution.Lifecycle.MinActiveDuration)
}
