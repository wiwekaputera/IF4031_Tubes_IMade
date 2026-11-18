module github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor

go 1.25.0

require (
	github.com/jackc/pgx/v5 v5.13.0
	github.com/nats-io/nats.go v1.36.0
	github.com/prometheus/client_golang v1.20.5
	github.com/wiwekaputera/if4031-tubes-imade/contracts v0.0.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	golang.org/x/crypto v0.18.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.22.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace github.com/wiwekaputera/if4031-tubes-imade/contracts => ../../contracts
