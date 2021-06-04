package poller

import (
	"context"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox"
	"time"
)

type repository interface {
	GetBatch() (*outbox.Batch, error)
}

func New(r repository, ch chan<- *outbox.Batch, ctx context.Context) *Poller {
	return &Poller{
		ch:   ch,
		repo: r,
		ctx:  ctx,
	}
}

type Poller struct {
	ch   chan<- *outbox.Batch
	repo repository
	ctx  context.Context
}

func (p Poller) Poll(interval time.Duration) {
	for {
		batch, err := p.repo.GetBatch()
		if err != nil {
			log.Logger.WithError(err).Errorf("an unexpected error occurred when polling the outbox: %s", err)
			time.Sleep(interval)
			continue
		}

		select {
		case p.ch <- batch:
			break
		case <-p.ctx.Done():
			return
		}

		time.Sleep(interval)
	}
}
