package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	sleepDur := flag.Duration("sleep", 0, "if > 0, sleep for this duration and exit 0 (for distroless preStop hooks)")
	flag.Parse()
	if *sleepDur > 0 {
		time.Sleep(*sleepDur)
		os.Exit(0)
	}

	url := os.Getenv("HEALTHCHECK_URL")
	if url == "" {
		url = "http://localhost:8080/health"
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		fmt.Fprintf(os.Stderr, "healthcheck: %s -> %d\n", url, resp.StatusCode)
		os.Exit(0)
	}

	fmt.Fprintf(os.Stderr, "healthcheck: %s -> %d\n", url, resp.StatusCode)
	os.Exit(1)
}
