// Package config manages runtime configuration for the pvmbg-mock service.
package config

import (
	"fmt"
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

// Load reads configuration from environment variables with fail-fast
// validation: the credential is required and numeric settings must be valid
// integers within range. Everything else has a sensible default.
func Load() (*Config, error) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	bearerToken := os.Getenv("PVMBG_BEARER_TOKEN")
	if bearerToken == "" {
		return nil, fmt.Errorf("PVMBG_BEARER_TOKEN environment variable is required")
	}

	delayMinMs, err := intEnv("PVMBG_DELAY_MIN_MS", 500, 0)
	if err != nil {
		return nil, err
	}
	delayMaxMs, err := intEnv("PVMBG_DELAY_MAX_MS", 3000, 0)
	if err != nil {
		return nil, err
	}
	if delayMaxMs < delayMinMs {
		return nil, fmt.Errorf("invalid PVMBG_DELAY_MAX_MS %d: must be >= PVMBG_DELAY_MIN_MS %d", delayMaxMs, delayMinMs)
	}

	eventIntervalS, err := intEnv("PVMBG_EVENT_INTERVAL_S", 10, 1)
	if err != nil {
		return nil, err
	}

	defaultLimit, err := intEnv("PVMBG_DEFAULT_PAGE_LIMIT", DefaultPageLimit, 1)
	if err != nil {
		return nil, err
	}
	maxLimit, err := intEnv("PVMBG_MAX_PAGE_LIMIT", MaxPageLimit, 1)
	if err != nil {
		return nil, err
	}
	if defaultLimit > maxLimit {
		return nil, fmt.Errorf("invalid PVMBG_DEFAULT_PAGE_LIMIT %d: must be <= PVMBG_MAX_PAGE_LIMIT %d", defaultLimit, maxLimit)
	}

	storeMaxCapacity, err := intEnv("STORE_MAX_CAPACITY", MaxStoreCapacity, MinStoreCapacity)
	if err != nil {
		return nil, err
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
	}, nil
}

// intEnv returns defaultVal when the variable is unset, otherwise the parsed
// integer, which must be >= minVal.
func intEnv(name string, defaultVal, minVal int) (int, error) {
	valStr := os.Getenv(name)
	if valStr == "" {
		return defaultVal, nil
	}
	val, err := strconv.Atoi(valStr)
	if err != nil || val < minVal {
		return 0, fmt.Errorf("invalid %s %q: must be integer >= %d", name, valStr, minVal)
	}
	return val, nil
}
