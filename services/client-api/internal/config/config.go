package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all configuration parameters for client-api.
type Config struct {
	Port              string
	AggregatorBaseURL string
	JWKSURL           string
	RPSLimit          float64
	Burst             int
	TokenIssuer       string
	TokenAudience     string
	LogLevel          string
}

// Load reads configuration from environment variables with sensible defaults.
func Load() (*Config, error) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	aggregatorBaseURL := os.Getenv("AGGREGATOR_BASE_URL")
	if aggregatorBaseURL == "" {
		aggregatorBaseURL = "http://aggregator:8090"
	}

	jwksURL := os.Getenv("JWKS_URL")
	if jwksURL == "" {
		jwksURL = "http://token-service:8083/.well-known/jwks.json"
	}

	rpsLimit := 100.0
	if rpsStr := os.Getenv("RPS_LIMIT"); rpsStr != "" {
		parsed, err := strconv.ParseFloat(rpsStr, 64)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("invalid RPS_LIMIT %q: must be positive number", rpsStr)
		}
		rpsLimit = parsed
	}

	burst := 20
	if burstStr := os.Getenv("BURST"); burstStr != "" {
		parsed, err := strconv.Atoi(burstStr)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("invalid BURST %q: must be positive integer", burstStr)
		}
		burst = parsed
	}

	tokenIssuer := os.Getenv("TOKEN_ISSUER")
	if tokenIssuer == "" {
		tokenIssuer = "bnpb-token-service"
	}

	tokenAudience := os.Getenv("TOKEN_AUDIENCE")
	if tokenAudience == "" {
		tokenAudience = "bnpb-client-api"
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	return &Config{
		Port:              port,
		AggregatorBaseURL: aggregatorBaseURL,
		JWKSURL:           jwksURL,
		RPSLimit:          rpsLimit,
		Burst:             burst,
		TokenIssuer:       tokenIssuer,
		TokenAudience:     tokenAudience,
		LogLevel:          logLevel,
	}, nil
}
