package kafka

import (
	"inviqa/kafka-outbox-relay/outbox"

	"github.com/Shopify/sarama"
)

type MessageExpectation struct {
	Msg     *outbox.Message
	Headers []*sarama.RecordHeader
	Key     []byte
}
