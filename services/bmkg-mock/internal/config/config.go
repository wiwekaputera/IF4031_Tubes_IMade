package config

import (
	"os"
	"strconv"
)

// DefaultPageLimit is the number of records returned when the client omits ?limit.
const DefaultPageLimit = 50

// MaxPageLimit is the hard ceiling to prevent clients from requesting huge pages.
const MaxPageLimit = 100

// MaxStoreCapacity is the default upper bound on the number of events retained in
// the in-memory store. At ~5,000 records this retains under 1 MB of RAM while
// keeping the store well above the initial seed count (≥20 records) so that seed
// data is never prematurely evicted at startup.
const MaxStoreCapacity = 5000

// MinStoreCapacity is the smallest retention capacity allowed via configuration.
// It is intentionally larger than the seed count so historical seed records remain
// available until dynamically generated events displace them over many ticks.
const MinStoreCapacity = 20

// Config holds the runtime configuration parameters for the bmkg-mock service.
type Config struct {
	Port             string
	APIKey           string
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
		port = "8081"
	}

	apiKey := os.Getenv("BMKG_API_KEY")

	delayMinMs := getEnvAsInt("BMKG_DELAY_MIN_MS", 50)
	delayMaxMs := getEnvAsInt("BMKG_DELAY_MAX_MS", 150)
	if delayMaxMs < delayMinMs {
		delayMaxMs = delayMinMs
	}

	eventIntervalS := getEnvAsInt("BMKG_EVENT_INTERVAL_S", 10)
	if eventIntervalS <= 0 {
		eventIntervalS = 10
	}

	defaultLimit := getEnvAsInt("BMKG_DEFAULT_PAGE_LIMIT", DefaultPageLimit)
	if defaultLimit < 1 {
		defaultLimit = DefaultPageLimit
	}
	maxLimit := getEnvAsInt("BMKG_MAX_PAGE_LIMIT", MaxPageLimit)
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
		APIKey:           apiKey,
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
