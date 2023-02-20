package processor

import (
	"context"
	"errors"
	"io"

	nr "github.com/newrelic/go-agent/v3/newrelic"

	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/newrelic"
	"inviqa/kafka-outbox-relay/outbox"

	"github.com/sirupsen/logrus"
)

type repository interface {
	CommitBatch(ctx context.Context, batch *outbox.Batch)
}

type publisher interface {
	io.Closer
	PublishMessage(m *outbox.Message) error
}

func NewBatchProcessor(r repository, p publisher, nrApp *nr.Application) KafkaBatchProcessor {
	return KafkaBatchProcessor{
		repo:      r,
		publisher: p,
		nrApp:     nrApp,
	}
}

type KafkaBatchProcessor struct {
	repo      repository
	publisher publisher
	nrApp     *nr.Application
}

func (k KafkaBatchProcessor) ListenAndProcess(parent context.Context, batches <-chan *outbox.Batch) {
	for {
		select {
		case b := <-batches:
			if b == nil || len(b.Messages) == 0 {
				break
			}

			ctx, txn := newrelic.ContextWithTxn(parent, "processor: KafkaBatchProcessor.ListenAndProcess()", k.nrApp)
			for _, msg := range b.Messages {
				if msg.Topic == "" {
					log.Logger.WithFields(logrus.Fields{"message_id": msg.Id}).Error("a message without a topic was detected in the outbox")
					err := errors.New("this message has no topic")
					msg.ErrorReason = err
					txn.NoticeError(err)
				} else {
					log.Logger.WithFields(logrus.Fields{"message": msg}).Debug("sending message to Kafka publisher")
					if err := k.publisher.PublishMessage(msg); err != nil {
						log.Logger.WithError(err).Debug("error encountered whilst publishing a batch message to Kafka")
						msg.ErrorReason = err
						txn.NoticeError(err)
					}
				}
			}
			k.repo.CommitBatch(ctx, b)
			txn.End()
			break
		case <-parent.Done():
			return
		}
	}
}
