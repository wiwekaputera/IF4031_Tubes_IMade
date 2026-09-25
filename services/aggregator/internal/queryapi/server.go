package queryapi

import (
	"io"
	"log"
	"net/http"
	"time"
)

func Server(handler http.Handler, addr string) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          log.New(io.Discard, "", 0),
	}
}
