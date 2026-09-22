module github.com/wiwekaputera/if4031-tubes-imade/aggregator

go 1.23

require (
	github.com/wiwekaputera/if4031-tubes-imade/contracts v0.0.0
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.6.0
	github.com/nats-io/nats.go v1.36.0
	github.com/rs/zerolog v1.33.0
	go.etcd.io/bbolt v1.3.10
	golang.org/x/sync v0.7.0
)

replace github.com/wiwekaputera/if4031-tubes-imade/contracts => ../../contracts
