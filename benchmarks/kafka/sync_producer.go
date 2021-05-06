// +build benchmarks

package kafka

import (
	"inviqa/kafka-outbox-relay/kafka"
	"sync"

	"github.com/Shopify/sarama"
)

type SyncProducer struct {
	sync.RWMutex
	realSyncProducer sarama.SyncProducer
	msgsPublished    int
}

func NewSyncProducer(kafkaHost []string) *SyncProducer {
	rp, err := sarama.NewSyncProducer(kafkaHost, kafka.NewSaramaConfig(false, false))
	if err != nil {
		panic(err)
	}

	return &SyncProducer{
		realSyncProducer: rp,
	}
}

func (sp *SyncProducer) GetMessagesPublishedCount() int {
	sp.RLock()
	defer sp.RUnlock()
	return sp.msgsPublished
}

func (sp *SyncProducer) ResetMessagesPublishedCount() {
	sp.Lock()
	defer sp.Unlock()
	sp.msgsPublished = 0
}

func (sp *SyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	sp.RLock()
	defer sp.RUnlock()

	pt, off, err := sp.realSyncProducer.SendMessage(msg)
	sp.msgsPublished++

	return pt, off, err
}

func (sp *SyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return nil
}

func (sp *SyncProducer) Close() error {
	return nil
}
