package poller

import (
	"context"
	"time"

	nr "github.com/newrelic/go-agent/v3/newrelic"

	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/newrelic"
	"inviqa/kafka-outbox-relay/outbox"
)

type repository interface {
	GetBatch(ctx context.Context) (*outbox.Batch, error)
}

func New(r repository, ch chan<- *outbox.Batch, nrApp *nr.Application) *Poller {
	return &Poller{
		ch:    ch,
		repo:  r,
		nrApp: nrApp,
	}
}

type Poller struct {
	ch    chan<- *outbox.Batch
	repo  repository
	nrApp *nr.Application
}

func (p Poller) Poll(parent context.Context, backoff time.Duration) {
	for {
		ctx, txn := newrelic.ContextWithTxn(parent, "outbox: Poller.Poll()", p.nrApp)
		batch, err := p.repo.GetBatch(ctx)
		if err != nil {
			if err != outbox.ErrNoEvents {
				log.Logger.WithError(err).Errorf("an unexpected error occurred when polling the outbox: %s", err)
			}
			txn.NoticeError(err)
			txn.End()
			time.Sleep(backoff)
			continue
		}
		txn.End()

		select {
		case p.ch <- batch:
			break
		case <-parent.Done():
			return
		}
	}
}
