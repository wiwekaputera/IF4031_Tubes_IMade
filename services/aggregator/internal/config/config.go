// Package config manages runtime configuration for the aggregator service.
package config

import (
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

// Load loads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		Port:                 getEnv("PORT", "8090"),
		BMKGBaseURL:          getEnv("BMKG_BASE_URL", "http://bmkg-mock:8081"),
		BMKGAPIKey:           getEnv("BMKG_API_KEY", ""),
		BMKGTimeout:          getDurationEnv("BMKG_TIMEOUT_MS", 1000, time.Millisecond),
		PVMBGBaseURL:         getEnv("PVMBG_BASE_URL", "http://pvmbg-mock:8082"),
		PVMBGBearerToken:     getEnv("PVMBG_BEARER_TOKEN", ""),
		PVMBGTimeout:         getDurationEnv("PVMBG_TIMEOUT_MS", 4000, time.Millisecond),
		PVMBGCbThreshold:     getIntEnv("PVMBG_CB_THRESHOLD", 3),
		PVMBGCbProbeInterval: getDurationEnv("PVMBG_CB_PROBE_INTERVAL", 30, time.Second),
		PollInterval:         getDurationEnv("POLL_INTERVAL_S", 3, time.Second),
		DBDSN:                getEnv("DB_DSN", ""),
		NATSURL:              getEnv("NATS_URL", "nats://nats-jetstream:4222"),
		OutboxPollInterval:   getDurationEnv("OUTBOX_POLL_INTERVAL_MS", 500, time.Millisecond),
		OutboxBatchSize:      getIntEnv("OUTBOX_BATCH_SIZE", 100),
		LogLevel:             strings.ToLower(getEnv("LOG_LEVEL", "info")),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getIntEnv(key string, defaultVal int) int {
	val := getEnv(key, "")
	if val == "" {
		return defaultVal
	}
	if n, err := strconv.Atoi(val); err == nil {
		return n
	}
	return defaultVal
}

func getDurationEnv(key string, defaultVal int, unit time.Duration) time.Duration {
	return time.Duration(getIntEnv(key, defaultVal)) * unit
}
