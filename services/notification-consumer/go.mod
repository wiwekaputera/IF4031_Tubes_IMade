module github.com/wiwekaputera/if4031-tubes-imade/notification-consumer

go 1.23

require (
	github.com/wiwekaputera/if4031-tubes-imade/contracts v0.0.0
	github.com/nats-io/nats.go v1.36.0
	github.com/rs/zerolog v1.33.0
	go.etcd.io/bbolt v1.3.10
)

replace github.com/wiwekaputera/if4031-tubes-imade/contracts => ../../contracts
