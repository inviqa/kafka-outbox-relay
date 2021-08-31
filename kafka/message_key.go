package kafka

import (
	"github.com/Shopify/sarama"
)

type MessageKey struct {
	// Key represents the actual key that is stored on the message in Kafka.
	// It is used by Kafka when doing certain things, like log compaction.
	Key string
	// PartitionKey is used to determine what partition this message will be
	// sent to in the topic. This can be empty, and if it is, the Key field
	// will be used instead. See KeyForPartitioning.
	PartitionKey string
	sarama.StringEncoder
}

// KeyForPartitioning returns the key field value that should be used to
// determine which partition the message should be sent to. If PartitionKey
// is empty, then the Key field value will be returned instead.
func (mk MessageKey) KeyForPartitioning() string {
	var key string
	if mk.PartitionKey == "" {
		key = mk.Key
	} else {
		key = mk.PartitionKey
	}
	return key
}

func newMessageKey(key, partitionKey string) MessageKey {
	return MessageKey{
		Key:           key,
		PartitionKey:  partitionKey,
		StringEncoder: sarama.StringEncoder(key),
	}
}
