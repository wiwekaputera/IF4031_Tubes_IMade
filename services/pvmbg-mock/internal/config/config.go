package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port            string
	BearerToken     string
	DelayMinMs      int
	DelayMaxMs      int
	EventIntervalS  int
	LogLevel        string
}

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

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	return &Config{
		Port:           port,
		BearerToken:    bearerToken,
		DelayMinMs:     delayMinMs,
		DelayMaxMs:     delayMaxMs,
		EventIntervalS: eventIntervalS,
		LogLevel:       logLevel,
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
