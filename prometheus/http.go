package prometheus

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"inviqa/kafka-outbox-relay/config"
	h "inviqa/kafka-outbox-relay/http"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox/data"
)

func StartHttpServer(ctx context.Context, cfg *config.Config, dbs data.DBs) {
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

	pingers := make([]h.Pinger, len(dbs))
	i := 0
	dbs.Each(func(db data.DB) {
		pingers[i] = db.Connection()
		i++
	})

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/healthz", h.NewHealthzHandler(cfg.GetDependencySystemAddresses(), pingers))
	srv = http.Server{
		Handler: mux,
		Addr:    ":80",
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Logger.Fatalf("failed to start HTTP server: %s", err)
	}
}
