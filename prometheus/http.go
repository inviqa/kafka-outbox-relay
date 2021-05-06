package prometheus

import (
	"inviqa/kafka-outbox-relay/config"
	h "inviqa/kafka-outbox-relay/http"
	"inviqa/kafka-outbox-relay/log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func StartHttpServer(cfg *config.Config, db h.Pinger) {
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/healthz", h.NewHealthzHandler(cfg.GetDependencySystemAddresses(), db))

	err := http.ListenAndServe(":80", nil)
	if err != nil {
		log.Logger.Fatalf("failed to start prometheus HTTP server: %s", err)
	}
}
