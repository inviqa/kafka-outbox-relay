package prometheus

import (
	"context"
	"inviqa/kafka-outbox-relay/log"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var outboxTotalSize prom.Gauge

type totalSizer interface {
	GetTotalSize() (uint, error)
}

func init() {
	outboxTotalSize = promauto.NewGauge(prom.GaugeOpts{
		Name: "kafka_outbox_total_size",
		Help: "The total size of the outbox (all messages)",
	})
}

func ObserveTotalSize(repo totalSizer, ctx context.Context) {
	for {
		size, err := repo.GetTotalSize()
		if err != nil {
			log.Logger.WithError(err).Error("an error occurred determining the size of the queue")
			time.Sleep(time.Second * 1)
			continue
		}

		select {
		case <-ctx.Done():
			return
		default:
			outboxTotalSize.Set(float64(size))

			time.Sleep(time.Second * 1)
		}
	}
}
