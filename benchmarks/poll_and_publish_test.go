// +build benchmarks

package benchmarks

import (
	"context"
	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/processor"
	"testing"
)

const (
	numMessagesToPopulateOutboxWith = 10000
	// beware when changing this value, if you are doing benchmarks between 2 different
	// implementations then this values should remain the same for each benchmark run
	batchSize = 50
)

func BenchmarkOutboxPollAndPublishToKafka(b *testing.B) {
	cfg.BatchSize = batchSize
	repo = outbox.NewRepository(db, cfg)
	batchCh := make(chan *outbox.Batch)
	proc := processor.NewBatchProcessor(repo, pub, context.Background())
	go proc.ListenAndProcess(batchCh)

	purgeOutboxTable()
	populateOutbox()
	b.ResetTimer()

	// this simulates the poller, we can't use the real poller.Poller implementation
	// because it is designed to be a long-running process and it's difficult to reliably
	// measure the performance of it, instead we implement the same simple for loop here
	// but wait for the Kafka producer to publish all messages in the batch
	for i := 0; i < b.N; i++ {
		batch, err := repo.GetBatch()
		if err != nil {
			b.Fatalf("an error occurred during repo.GetBatch(): %s", err)
		}

		batchCh <- batch
		// wait for the messages to be published to Kafka
		for {
			if syncProducer.GetMessagesPublishedCount() == batchSize {
				syncProducer.ResetMessagesPublishedCount()
				break
			}
		}
	}
}

func populateOutbox() {
	var msgs []*outbox.Message
	for i := 0; i < numMessagesToPopulateOutboxWith; i++ {
		msg := &outbox.Message{
			PayloadJson: []byte(`{"foo": "bar"}`),
			Topic:       "testProductUpdate",
		}
		msgs = append(msgs, msg)
	}

	insertOutboxMessages(msgs)
}
