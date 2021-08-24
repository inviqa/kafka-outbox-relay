package kafka

import (
	"github.com/Shopify/sarama"
)

type MessageKey struct {
	Key                  string
	PartitionKey         string
	sarama.StringEncoder
}

func newMessageKey(key, partitionKey string) MessageKey {
	return MessageKey{
		Key:          key,
		PartitionKey: partitionKey,
		StringEncoder: sarama.StringEncoder(key),
	}
}
