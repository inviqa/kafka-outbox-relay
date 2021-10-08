package prometheus

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"inviqa/kafka-outbox-relay/config"
	h "inviqa/kafka-outbox-relay/http"
	"inviqa/kafka-outbox-relay/log"
)

func StartHttpServer(ctx context.Context, cfg *config.Config, db h.Pinger) {
	var srv http.Server

	go func() {
		select {
		case _ = <-ctx.Done():
			httpCtx, httpCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer httpCancel()
			if err := srv.Shutdown(httpCtx); err != nil {
				log.Logger.WithError(err).Error("HTTP server did not shutdown correctly")
			}
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/healthz", h.NewHealthzHandler(cfg.GetDependencySystemAddresses(), db))
	srv = http.Server{
		Handler: mux,
		Addr:    ":80",
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Logger.Fatalf("failed to start HTTP server: %s", err)
	}
}
