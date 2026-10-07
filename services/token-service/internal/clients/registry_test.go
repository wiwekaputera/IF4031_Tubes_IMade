package clients

import (
	"testing"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/config"
)

func TestRegistry(t *testing.T) {
	cfg := &config.Config{
		ClientMediaID:           "media-client",
		ClientMediaSecret:       "media-secret",
		ClientFieldTeamID:       "field-client",
		ClientFieldTeamSecret:   "field-secret",
		ClientInternalOpsID:     "ops-client",
		ClientInternalOpsSecret: "ops-secret",
	}

	reg := NewRegistry(cfg)

	// Valid authentication
	client, ok := reg.Authenticate("media-client", "media-secret")
	if !ok || client.Scope != auth.ScopeMedia || client.AllowRefreshToken {
		t.Fatalf("unexpected media client: %+v", client)
	}

	fieldClient, ok := reg.Authenticate("field-client", "field-secret")
	if !ok || fieldClient.Scope != auth.ScopeFieldTeam || !fieldClient.AllowRefreshToken {
		t.Fatalf("unexpected field client: %+v", fieldClient)
	}

	opsClient, ok := reg.Authenticate("ops-client", "ops-secret")
	if !ok || opsClient.Scope != auth.ScopeInternalOps || opsClient.AllowRefreshToken {
		t.Fatalf("unexpected ops client: %+v", opsClient)
	}

	// Invalid password
	if _, ok := reg.Authenticate("media-client", "wrong-secret"); ok {
		t.Fatal("expected failure on wrong secret")
	}

	// Unknown client
	if _, ok := reg.Authenticate("unknown", "secret"); ok {
		t.Fatal("expected failure on unknown client")
	}
}
