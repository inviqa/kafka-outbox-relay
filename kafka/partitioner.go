package kafka

import (
	"github.com/Shopify/sarama"
)

type OutboxPartitioner struct {
	topic           string
	hashPartitioner sarama.Partitioner
}

func NewOutboxPartitioner(topic string) sarama.Partitioner {
	return NewOutboxPartitionerWithCustomPartitioner(topic, sarama.NewHashPartitioner(topic))
}

func NewOutboxPartitionerWithCustomPartitioner(topic string, p sarama.Partitioner) sarama.Partitioner {
	return OutboxPartitioner{
		topic:           topic,
		hashPartitioner: p,
	}
}

func (o OutboxPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	mk, ok := message.Key.(MessageKey)
	if !ok {
		return o.hashPartitioner.Partition(message, numPartitions)
	}

	var key string
	if mk.PartitionKey == "" {
		key = mk.Key
	} else {
		key = mk.PartitionKey
	}

	// set the key on the message temporarily and allow the hashPartitioner to
	// determine the partition for us, we will revert it back before we proceed
	// in case the sarama module decides to mutate the message in its hashPartitioner
	// implementation in the future
	message.Key = sarama.StringEncoder(key)

	ptn, err := o.hashPartitioner.Partition(message, numPartitions)

	// reset the message key back to what it was originally, just in case the sarama
	// module decides to mutate it in a future version
	message.Key = mk

	return ptn, err
}

func (o OutboxPartitioner) RequiresConsistency() bool {
	return true
}
