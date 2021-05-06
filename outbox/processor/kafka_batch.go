package processor

import (
	"context"
	"errors"
	"inviqa/kafka-outbox-relay/kafka"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox"

	"github.com/sirupsen/logrus"
)

type BatchProcessor interface {
	ListenAndProcess(batches <-chan *outbox.Batch)
}

type repository interface {
	CommitBatch(batch *outbox.Batch)
}

func NewBatchProcessor(r repository, p kafka.Publisher, ctx context.Context) BatchProcessor {
	return &kafkaBatchProcessor{
		repo:      r,
		publisher: p,
		ctx:       ctx,
	}
}

type kafkaBatchProcessor struct {
	repo      repository
	publisher kafka.Publisher
	ctx       context.Context
}

func (k kafkaBatchProcessor) ListenAndProcess(batches <-chan *outbox.Batch) {
	for {
		select {
		case b := <-batches:
			if b == nil || len(b.Messages) == 0 {
				break
			}

			for _, msg := range b.Messages {
				if msg.Topic == "" {
					log.Logger.WithFields(logrus.Fields{"message_id": msg.Id}).Error("a message without a topic was detected in the outbox")
					msg.ErrorReason = errors.New("this message has no topic")
				} else {
					log.Logger.WithFields(logrus.Fields{"message": msg}).Debug("sending message to Kafka publisher")
					if err := k.publisher.PublishMessage(msg); err != nil {
						log.Logger.WithError(err).Debug("error encountered whilst publishing a batch message to Kafka")
						msg.ErrorReason = err
					}
				}
			}
			k.repo.CommitBatch(b)
			break
		case <-k.ctx.Done():
			return
		}
	}
}
