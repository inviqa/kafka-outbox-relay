package prometheus

import (
	"context"
	"time"

	"inviqa/kafka-outbox-relay/log"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var outboxQueueSize prom.Gauge

type queueSizer interface {
	GetQueueSize() (uint, error)
}

func init() {
	outboxQueueSize = promauto.NewGauge(prom.GaugeOpts{
		Name: "kafka_outbox_queue_size",
		Help: "The current size of the outbox (all unpublished messages)",
	})
}

func ObserveQueueSize(sizer queueSizer, ctx context.Context) {
	for {
		size, err := sizer.GetQueueSize()
		if err != nil {
			log.Logger.WithError(err).Error("an error occurred determining the size of the queue")
			time.Sleep(backoffTime)
			continue
		}

		select {
		case <-ctx.Done():
			return
		default:
			outboxQueueSize.Set(float64(size))

			time.Sleep(backoffTime)
		}
	}
}
