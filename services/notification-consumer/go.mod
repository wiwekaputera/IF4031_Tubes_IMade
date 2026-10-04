module github.com/wiwekaputera/if4031-tubes-imade/notification-consumer

go 1.23

require (
	github.com/nats-io/nats.go v1.36.0
	github.com/rs/zerolog v1.33.0
	github.com/wiwekaputera/if4031-tubes-imade/contracts v0.0.0
	go.etcd.io/bbolt v1.3.10
)

require (
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.18.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
)

replace github.com/wiwekaputera/if4031-tubes-imade/contracts => ../../contracts
