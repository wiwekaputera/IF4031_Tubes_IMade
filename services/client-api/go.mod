module github.com/wiwekaputera/if4031-tubes-imade/client-api

go 1.23

require (
	github.com/wiwekaputera/if4031-tubes-imade/contracts v0.0.0
	github.com/golang-jwt/jwt/v5 v5.2.1
	github.com/rs/zerolog v1.33.0
	golang.org/x/time v0.5.0
)

replace github.com/wiwekaputera/if4031-tubes-imade/contracts => ../../contracts
