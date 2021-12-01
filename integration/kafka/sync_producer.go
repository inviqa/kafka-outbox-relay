//go:build integration
// +build integration

package kafka

import (
	"sync"

	"inviqa/kafka-outbox-relay/kafka"

	"github.com/Shopify/sarama"
)

type SyncProducer struct {
	sync.RWMutex
	realSyncProducer sarama.SyncProducer
	msgsToError      map[string]error
}

func NewSyncProducer(kafkaHost []string) *SyncProducer {
	rp, err := sarama.NewSyncProducer(kafkaHost, kafka.NewSaramaConfig(false, false))
	if err != nil {
		panic(err)
	}

	return &SyncProducer{
		realSyncProducer: rp,
		msgsToError:      map[string]error{},
	}
}

func (sp *SyncProducer) AddError(msgBody string, err error) {
	sp.Lock()
	defer sp.Unlock()
	sp.msgsToError[msgBody] = err
}

func (sp *SyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	sp.RLock()
	defer sp.RUnlock()
	b, err := msg.Value.Encode()
	if err != nil {
		panic(err)
	}
	err, ok := sp.msgsToError[string(b)]
	if ok {
		return 0, 0, err
	}

	return sp.realSyncProducer.SendMessage(msg)
}

func (sp *SyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return nil
}

func (sp *SyncProducer) Close() error {
	return nil
}
