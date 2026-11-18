module github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor

go 1.25.0

require (
	github.com/jackc/pgx/v5 v5.13.0
	github.com/nats-io/nats.go v1.36.0
	github.com/wiwekaputera/if4031-tubes-imade/contracts v0.0.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.18.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/wiwekaputera/if4031-tubes-imade/contracts => ../../contracts
