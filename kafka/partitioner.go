package kafka

import (
	"errors"

	"github.com/Shopify/sarama"
)

type OutboxPartitioner struct {
	hashPartitioner sarama.Partitioner
}

func NewOutboxPartitioner(topic string) sarama.Partitioner {
	return OutboxPartitioner{
		hashPartitioner: sarama.NewHashPartitioner(topic),
	}
}

func (o OutboxPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	mk, ok := message.Key.(MessageKey)
	if !ok {
		return o.hashPartitioner.Partition(message, numPartitions)
	}

	// TODO: use mk.PartitionKey to determine which partition, by hashing

	return 0, errors.New("not yet implemented")
}

// RequiresConsistency indicates to the user of the partitioner whether the
// mapping of key->partition is consistent or not. Specifically, if a
// partitioner requires consistency then it must be allowed to choose from all
// partitions (even ones known to be unavailable), and its choice must be
// respected by the caller. The obvious example is the HashPartitioner.
func (o OutboxPartitioner) RequiresConsistency() bool {
	return true
}
