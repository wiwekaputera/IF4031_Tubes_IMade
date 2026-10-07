package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

type Config struct {
	Port                string
	DBDSN               string
	PrivateKeyB64       string
	AccessTokenTTL      time.Duration
	RefreshTokenTTLDays int

	ClientMediaID           string
	ClientMediaSecret       string
	ClientFieldTeamID       string
	ClientFieldTeamSecret   string
	ClientInternalOpsID     string
	ClientInternalOpsSecret string

	TokenIssuer   string
	TokenAudience string
	LogLevel      string
}

func Load() (*Config, error) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8083"
	}

	dbDSN := os.Getenv("TOKEN_DB_DSN")
	if dbDSN == "" {
		return nil, fmt.Errorf("TOKEN_DB_DSN environment variable is required")
	}

	privKeyB64 := os.Getenv("ES256_PRIVATE_KEY_B64")
	if privKeyB64 == "" {
		return nil, fmt.Errorf("ES256_PRIVATE_KEY_B64 environment variable is required")
	}

	accessTTLSec := 60
	if ttlStr := os.Getenv("ACCESS_TOKEN_TTL"); ttlStr != "" {
		parsed, err := strconv.Atoi(ttlStr)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("invalid ACCESS_TOKEN_TTL %q: must be positive integer", ttlStr)
		}
		accessTTLSec = parsed
	}

	refreshTTLDays := 7
	if daysStr := os.Getenv("REFRESH_TOKEN_TTL_DAYS"); daysStr != "" {
		parsed, err := strconv.Atoi(daysStr)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("invalid REFRESH_TOKEN_TTL_DAYS %q: must be positive integer", daysStr)
		}
		refreshTTLDays = parsed
	}

	clientMediaID := os.Getenv("CLIENT_MEDIA_ID")
	if clientMediaID == "" {
		clientMediaID = "client-media"
	}
	clientMediaSecret := os.Getenv("CLIENT_MEDIA_SECRET")
	if clientMediaSecret == "" {
		return nil, fmt.Errorf("CLIENT_MEDIA_SECRET environment variable is required")
	}

	clientFieldTeamID := os.Getenv("CLIENT_FIELD_TEAM_ID")
	if clientFieldTeamID == "" {
		clientFieldTeamID = "client-field-team"
	}
	clientFieldTeamSecret := os.Getenv("CLIENT_FIELD_TEAM_SECRET")
	if clientFieldTeamSecret == "" {
		return nil, fmt.Errorf("CLIENT_FIELD_TEAM_SECRET environment variable is required")
	}

	clientInternalOpsID := os.Getenv("CLIENT_INTERNAL_OPS_ID")
	if clientInternalOpsID == "" {
		clientInternalOpsID = "client-internal-ops"
	}
	clientInternalOpsSecret := os.Getenv("CLIENT_INTERNAL_OPS_SECRET")
	if clientInternalOpsSecret == "" {
		return nil, fmt.Errorf("CLIENT_INTERNAL_OPS_SECRET environment variable is required")
	}

	issuer := os.Getenv("TOKEN_ISSUER")
	if issuer == "" {
		issuer = auth.DefaultIssuer
	}

	audience := os.Getenv("TOKEN_AUDIENCE")
	if audience == "" {
		audience = auth.DefaultAudience
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	return &Config{
		Port:                    port,
		DBDSN:                   dbDSN,
		PrivateKeyB64:           privKeyB64,
		AccessTokenTTL:          time.Duration(accessTTLSec) * time.Second,
		RefreshTokenTTLDays:     refreshTTLDays,
		ClientMediaID:           clientMediaID,
		ClientMediaSecret:       clientMediaSecret,
		ClientFieldTeamID:       clientFieldTeamID,
		ClientFieldTeamSecret:   clientFieldTeamSecret,
		ClientInternalOpsID:     clientInternalOpsID,
		ClientInternalOpsSecret: clientInternalOpsSecret,
		TokenIssuer:             issuer,
		TokenAudience:           audience,
		LogLevel:                logLevel,
	}, nil
}
