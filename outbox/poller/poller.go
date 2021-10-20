package poller

import (
	"context"
	"time"

	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox"
)

type repository interface {
	GetBatch() (*outbox.Batch, error)
}

func New(r repository, ch chan<- *outbox.Batch) *Poller {
	return &Poller{
		ch:   ch,
		repo: r,
	}
}

type Poller struct {
	ch   chan<- *outbox.Batch
	repo repository
}

func (p Poller) Poll(ctx context.Context, backoff time.Duration) {
	for {
		batch, err := p.repo.GetBatch()
		if err != nil {
			if err != outbox.ErrNoEvents {
				log.Logger.WithError(err).Errorf("an unexpected error occurred when polling the outbox: %s", err)
			}
			time.Sleep(backoff)
			continue
		}

		select {
		case p.ch <- batch:
			break
		case <-ctx.Done():
			return
		}
	}
}
