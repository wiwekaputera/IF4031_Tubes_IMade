package config

import (
	"testing"
	"time"
)

func setBaseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("BMKG_API_KEY", "k")
	t.Setenv("PVMBG_BEARER_TOKEN", "t")
	t.Setenv("DB_DSN", "postgresql://root@localhost:26257/canonical?sslmode=disable")
}

func TestLoad_LeaseDefaults(t *testing.T) {
	setBaseEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LeaseTTL != 15*time.Second {
		t.Errorf("expected LeaseTTL 15s, got %v", cfg.LeaseTTL)
	}
	if cfg.LeaseRenewInterval != 5*time.Second {
		t.Errorf("expected LeaseRenewInterval 5s, got %v", cfg.LeaseRenewInterval)
	}
	if cfg.ShutdownDrain != 5*time.Second {
		t.Errorf("expected ShutdownDrain 5s, got %v", cfg.ShutdownDrain)
	}
}

func TestLoad_RejectsLeaseTTLNotGreaterThanTwiceRenew(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("LEASE_TTL_S", "10")
	t.Setenv("LEASE_RENEW_INTERVAL_S", "5")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when LEASE_TTL_S <= 2*LEASE_RENEW_INTERVAL_S")
	}
}
