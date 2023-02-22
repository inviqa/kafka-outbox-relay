package prometheus

import (
	"context"
	"time"

	"inviqa/kafka-outbox-relay/log"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var outboxTotalSize prom.Gauge

func init() {
	outboxTotalSize = promauto.NewGauge(prom.GaugeOpts{
		Name: "kafka_outbox_total_size",
		Help: "The total size of the outbox (all messages)",
	})
}

func ObserveTotalSize(ctx context.Context, sizers []Sizer) {
	for {
		size := totalSize(sizers)

		select {
		case <-ctx.Done():
			return
		default:
			outboxTotalSize.Set(float64(size))
			time.Sleep(backoffTime)
		}
	}
}

func totalSize(sizers []Sizer) uint {
	var total uint
	for _, sizer := range sizers {
		size, err := sizer.GetTotalSize()
		if err != nil {
			log.Logger.WithError(err).Error("an error occurred determining the size of the queue")
			time.Sleep(backoffTime)
			continue
		}
		total += size
	}
	return total
}
