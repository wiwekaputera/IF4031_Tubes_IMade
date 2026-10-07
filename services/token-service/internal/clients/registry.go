package clients

import (
	"crypto/subtle"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/config"
)

// Client represents a registered downstream consumer.
type Client struct {
	ID                string
	Secret            string
	Scope             auth.Scope
	AllowRefreshToken bool
}

// Registry maintains the set of known OAuth 2.0 clients.
type Registry struct {
	clients map[string]Client
}

// NewRegistry initializes the registry with the three required clients from config.
func NewRegistry(cfg *config.Config) *Registry {
	r := &Registry{
		clients: make(map[string]Client),
	}

	r.clients[cfg.ClientMediaID] = Client{
		ID:                cfg.ClientMediaID,
		Secret:            cfg.ClientMediaSecret,
		Scope:             auth.ScopeMedia,
		AllowRefreshToken: false,
	}

	r.clients[cfg.ClientFieldTeamID] = Client{
		ID:                cfg.ClientFieldTeamID,
		Secret:            cfg.ClientFieldTeamSecret,
		Scope:             auth.ScopeFieldTeam,
		AllowRefreshToken: true,
	}

	r.clients[cfg.ClientInternalOpsID] = Client{
		ID:                cfg.ClientInternalOpsID,
		Secret:            cfg.ClientInternalOpsSecret,
		Scope:             auth.ScopeInternalOps,
		AllowRefreshToken: false,
	}

	return r
}

// Authenticate verifies client_id and client_secret using constant-time comparison to prevent timing attacks.
func (r *Registry) Authenticate(clientID, clientSecret string) (*Client, bool) {
	client, exists := r.clients[clientID]
	if !exists {
		return nil, false
	}

	secretMatch := subtle.ConstantTimeCompare([]byte(client.Secret), []byte(clientSecret)) == 1
	if !secretMatch {
		return nil, false
	}

	return &client, true
}

// Get looks up a client by ID without authenticating.
func (r *Registry) Get(clientID string) (*Client, bool) {
	client, exists := r.clients[clientID]
	if !exists {
		return nil, false
	}
	return &client, true
}
