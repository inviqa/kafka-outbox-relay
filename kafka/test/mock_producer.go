package test

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/go-test/deep"
)

type mockSyncProducer struct {
	producedMessages map[string][]*sarama.ProducerMessage
}

func NewMockSyncProducer() *mockSyncProducer {
	return &mockSyncProducer{
		producedMessages: map[string][]*sarama.ProducerMessage{},
	}
}

func (m *mockSyncProducer) MessageWasProduced(topic string, exp *sarama.ProducerMessage) error {
	if _, ok := m.producedMessages[topic]; !ok {
		return fmt.Errorf("0 messages produced for the %s topic", topic)
	}

	for _, msg := range m.producedMessages[topic] {
		if diff := deep.Equal(exp, msg); diff == nil {
			return nil
		}
	}
	return fmt.Errorf("no message published in topic %s that matches provided message %#v", topic, exp)
}

func (m *mockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	m.producedMessages[msg.Topic] = append(m.producedMessages[msg.Topic], msg)

	return 0, 0, nil
}

func (m *mockSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return nil
}

func (m *mockSyncProducer) Close() error {
	return nil
}
