// Package config manages runtime configuration for the pvmbg-mock service.
package config

import (
	"os"
	"strconv"
)

const (
	// DefaultPageLimit is the default page size when ?limit is omitted.
	DefaultPageLimit = 250

	// MaxPageLimit is the maximum page size allowed per request.
	MaxPageLimit = 1000

	// MaxStoreCapacity is the default capacity for the in-memory report store.
	MaxStoreCapacity = 100000

	// MinStoreCapacity is the minimum allowed retention capacity.
	MinStoreCapacity = 20
)

// Config holds runtime configuration parameters for the pvmbg-mock service.
type Config struct {
	Port             string
	BearerToken      string
	DelayMinMs       int
	DelayMaxMs       int
	EventIntervalS   int
	StoreMaxCapacity int
	DefaultPageLimit int
	MaxPageLimit     int
	LogLevel         string
}

// Load loads configuration from environment variables with sensible defaults.
func Load() *Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	bearerToken := os.Getenv("PVMBG_BEARER_TOKEN")

	delayMinMs := getEnvAsInt("PVMBG_DELAY_MIN_MS", 500)
	delayMaxMs := getEnvAsInt("PVMBG_DELAY_MAX_MS", 3000)
	if delayMaxMs < delayMinMs {
		delayMaxMs = delayMinMs
	}

	eventIntervalS := getEnvAsInt("PVMBG_EVENT_INTERVAL_S", 10)
	if eventIntervalS <= 0 {
		eventIntervalS = 10
	}

	defaultLimit := getEnvAsInt("PVMBG_DEFAULT_PAGE_LIMIT", DefaultPageLimit)
	if defaultLimit < 1 {
		defaultLimit = DefaultPageLimit
	}
	maxLimit := getEnvAsInt("PVMBG_MAX_PAGE_LIMIT", MaxPageLimit)
	if maxLimit < 1 {
		maxLimit = MaxPageLimit
	}
	if defaultLimit > maxLimit {
		defaultLimit = maxLimit
	}

	storeMaxCapacity := getEnvAsInt("STORE_MAX_CAPACITY", MaxStoreCapacity)
	if storeMaxCapacity < MinStoreCapacity {
		storeMaxCapacity = MaxStoreCapacity
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	return &Config{
		Port:             port,
		BearerToken:      bearerToken,
		DelayMinMs:       delayMinMs,
		DelayMaxMs:       delayMaxMs,
		EventIntervalS:   eventIntervalS,
		StoreMaxCapacity: storeMaxCapacity,
		DefaultPageLimit: defaultLimit,
		MaxPageLimit:     maxLimit,
		LogLevel:         logLevel,
	}
}

func getEnvAsInt(name string, defaultVal int) int {
	valStr := os.Getenv(name)
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return defaultVal
	}
	return val
}
