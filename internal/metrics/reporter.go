package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Reporter serves the /metrics endpoint for Prometheus scraping.
type Reporter struct {
	srv *http.Server
}

// NewReporter creates a Reporter that listens on addr.
// The /metrics path serves all default Go runtime metrics plus
// any RaftrixDB metrics registered by Collector.
func NewReporter(addr string) *Reporter {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return &Reporter{
		srv: &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
	}
}

// Start launches the metrics server in the background.
func (r *Reporter) Start() {
	go func() {
		slog.Info("metrics server listening", "addr", r.srv.Addr)
		if err := r.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server error", "err", err)
		}
	}()
}

// Stop gracefully shuts the metrics server down.
func (r *Reporter) Stop(ctx context.Context) error {
	return r.srv.Shutdown(ctx)
}