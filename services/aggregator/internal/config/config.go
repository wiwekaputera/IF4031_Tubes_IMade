// Package config manages runtime configuration for the aggregator service.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime configuration parameters for the aggregator service.
type Config struct {
	Port                 string
	BMKGBaseURL          string
	BMKGAPIKey           string
	BMKGTimeout          time.Duration
	PVMBGBaseURL         string
	PVMBGBearerToken     string
	PVMBGTimeout         time.Duration
	PVMBGCbThreshold     int
	PVMBGCbProbeInterval time.Duration
	PollInterval         time.Duration
	DBDSN                string
	NATSURL              string
	OutboxPollInterval   time.Duration
	OutboxBatchSize      int
	LogLevel             string
}

// Load reads configuration from environment variables with fail-fast
// validation: credentials and the database DSN are required, and numeric
// settings must be positive integers. Everything else has a sensible default.
func Load() (*Config, error) {
	bmkgAPIKey, err := requireEnv("BMKG_API_KEY")
	if err != nil {
		return nil, err
	}
	pvmbgBearerToken, err := requireEnv("PVMBG_BEARER_TOKEN")
	if err != nil {
		return nil, err
	}
	dbDSN, err := requireEnv("DB_DSN")
	if err != nil {
		return nil, err
	}

	bmkgTimeoutMs, err := positiveIntEnv("BMKG_TIMEOUT_MS", 1000)
	if err != nil {
		return nil, err
	}
	pvmbgTimeoutMs, err := positiveIntEnv("PVMBG_TIMEOUT_MS", 4000)
	if err != nil {
		return nil, err
	}
	cbThreshold, err := positiveIntEnv("PVMBG_CB_THRESHOLD", 3)
	if err != nil {
		return nil, err
	}
	cbProbeIntervalS, err := positiveIntEnv("PVMBG_CB_PROBE_INTERVAL", 30)
	if err != nil {
		return nil, err
	}
	pollIntervalS, err := positiveIntEnv("POLL_INTERVAL_S", 3)
	if err != nil {
		return nil, err
	}
	outboxPollIntervalMs, err := positiveIntEnv("OUTBOX_POLL_INTERVAL_MS", 500)
	if err != nil {
		return nil, err
	}
	outboxBatchSize, err := positiveIntEnv("OUTBOX_BATCH_SIZE", 100)
	if err != nil {
		return nil, err
	}

	return &Config{
		Port:                 getEnv("PORT", "8090"),
		BMKGBaseURL:          getEnv("BMKG_BASE_URL", "http://bmkg-mock:8081"),
		BMKGAPIKey:           bmkgAPIKey,
		BMKGTimeout:          time.Duration(bmkgTimeoutMs) * time.Millisecond,
		PVMBGBaseURL:         getEnv("PVMBG_BASE_URL", "http://pvmbg-mock:8082"),
		PVMBGBearerToken:     pvmbgBearerToken,
		PVMBGTimeout:         time.Duration(pvmbgTimeoutMs) * time.Millisecond,
		PVMBGCbThreshold:     cbThreshold,
		PVMBGCbProbeInterval: time.Duration(cbProbeIntervalS) * time.Second,
		PollInterval:         time.Duration(pollIntervalS) * time.Second,
		DBDSN:                dbDSN,
		NATSURL:              getEnv("NATS_URL", "nats://nats-jetstream:4222"),
		OutboxPollInterval:   time.Duration(outboxPollIntervalMs) * time.Millisecond,
		OutboxBatchSize:      outboxBatchSize,
		LogLevel:             strings.ToLower(getEnv("LOG_LEVEL", "info")),
	}, nil
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func requireEnv(key string) (string, error) {
	val := os.Getenv(key)
	if val == "" {
		return "", fmt.Errorf("%s environment variable is required", key)
	}
	return val, nil
}

func positiveIntEnv(key string, defaultVal int) (int, error) {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal, nil
	}
	n, err := strconv.Atoi(val)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid %s %q: must be positive integer", key, val)
	}
	return n, nil
}
