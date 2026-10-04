module github.com/wiwekaputera/if4031-tubes-imade/aggregator

go 1.25.0

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.11.0
	github.com/nats-io/nats.go v1.36.0
	github.com/rs/zerolog v1.33.0
	github.com/wiwekaputera/if4031-tubes-imade/contracts v0.0.0
	golang.org/x/sync v0.17.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.18.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/wiwekaputera/if4031-tubes-imade/contracts => ../../contracts
