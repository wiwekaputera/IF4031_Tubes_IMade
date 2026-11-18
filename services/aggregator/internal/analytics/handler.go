package analytics

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var staticFS embed.FS

// Handler serves the embedded analytics dashboard at /analytics/.
type Handler struct {
	fileServer http.Handler
}

// New constructs an analytics dashboard Handler backed by embedded static assets.
func New() *Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	return &Handler{
		fileServer: http.StripPrefix("/analytics/", http.FileServer(http.FS(sub))),
	}
}

// RegisterRoutes attaches /analytics and /analytics/ to mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/analytics", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/analytics/", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/analytics/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// Clear any default Content-Type set by upstream middleware so http.FileServer
		// sets the proper MIME type for HTML, CSS, and JS assets.
		w.Header().Del("Content-Type")
		h.fileServer.ServeHTTP(w, r)
	})
}
