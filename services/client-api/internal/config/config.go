package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

// Config holds all configuration parameters for client-api.
type Config struct {
	Port              string
	AggregatorBaseURL string
	AggregatorTimeout time.Duration
	JWKSURL           string
	RPSLimit          float64
	Burst             int
	TokenIssuer       string
	TokenAudience     string
	LogLevel          string

	// NATSURL is the JetStream broker that stores accepted field reports.
	NATSURL string
	// FieldReportPublishTimeout bounds the wait for the JetStream ack before
	// answering 503 instead of 202.
	FieldReportPublishTimeout time.Duration

	// MaxInflight is the number of requests served at once before load
	// shedding starts; MediaInflightShare is the part media may use.
	MaxInflight        int
	MediaInflightShare float64
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

	aggregatorTimeout := 3 * time.Second
	if v := os.Getenv("AGGREGATOR_TIMEOUT_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms <= 0 {
			return nil, fmt.Errorf("invalid AGGREGATOR_TIMEOUT_MS %q: must be positive integer", v)
		}
		aggregatorTimeout = time.Duration(ms) * time.Millisecond
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
		tokenIssuer = contractsauth.DefaultIssuer
	}

	tokenAudience := os.Getenv("TOKEN_AUDIENCE")
	if tokenAudience == "" {
		tokenAudience = contractsauth.DefaultAudience
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://nats-jetstream:4222"
	}

	publishTimeout := 2 * time.Second
	if v := os.Getenv("FIELD_REPORT_PUBLISH_TIMEOUT_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms <= 0 {
			return nil, fmt.Errorf("invalid FIELD_REPORT_PUBLISH_TIMEOUT_MS %q: must be positive integer", v)
		}
		publishTimeout = time.Duration(ms) * time.Millisecond
	}

	maxInflight := 256
	if v := os.Getenv("MAX_INFLIGHT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid MAX_INFLIGHT %q: must be positive integer", v)
		}
		maxInflight = n
	}
	mediaShare := 0.6
	if v := os.Getenv("MEDIA_INFLIGHT_SHARE"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f <= 0 || f > 1 {
			return nil, fmt.Errorf("invalid MEDIA_INFLIGHT_SHARE %q: must be in (0, 1]", v)
		}
		mediaShare = f
	}

	return &Config{
		Port:              port,
		AggregatorBaseURL: aggregatorBaseURL,
		AggregatorTimeout: aggregatorTimeout,
		JWKSURL:           jwksURL,
		RPSLimit:          rpsLimit,
		Burst:             burst,
		TokenIssuer:       tokenIssuer,
		TokenAudience:     tokenAudience,
		LogLevel:          logLevel,

		NATSURL:                   natsURL,
		FieldReportPublishTimeout: publishTimeout,
		MaxInflight:               maxInflight,
		MediaInflightShare:        mediaShare,
	}, nil
}
