module github.com/wiwekaputera/if4031-tubes-imade/client-api

go 1.26.0

replace github.com/wiwekaputera/if4031-tubes-imade/contracts => ../../contracts

require (
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/rs/zerolog v1.35.1
	github.com/wiwekaputera/if4031-tubes-imade/contracts v0.0.0-00010101000000-000000000000
	golang.org/x/sync v0.23.0
	golang.org/x/time v0.16.0
)

require (
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.29.0 // indirect
)
