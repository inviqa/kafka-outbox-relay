package kafka

import (
	"errors"
	"reflect"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/go-test/deep"
)

func TestNewOutboxPartitioner(t *testing.T) {
	got := NewOutboxPartitioner("foo")

	op := got.(OutboxPartitioner)
	if op.topic != "foo" {
		t.Errorf("expected 'foo' as topic but got '%s'", op.topic)
	}
}

func TestNewOutboxPartitionerWithCustomPartitioner(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	fp := newFakeHashPartitioner(false)
	got := NewOutboxPartitionerWithCustomPartitioner("bar", fp)

	exp := OutboxPartitioner{
		topic:           "bar",
		hashPartitioner: fp,
	}

	if diff := deep.Equal(exp, got); diff != nil {
		t.Error(diff)
	}
}

func TestOutboxPartitioner_Partition(t *testing.T) {
	t.Run("partition key is used on message key", func(t *testing.T) {
		t.Parallel()
		fp := newFakeHashPartitioner(false)
		fp.partitionToReturn = 9
		ob := NewOutboxPartitionerWithCustomPartitioner("product", fp)
		mk := newMessageKey("foo", "bar")
		msg := &sarama.ProducerMessage{Key: mk}

		got, err := ob.Partition(msg, 10)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if got != 9 {
			t.Errorf("expected partition 9 but got %d", got)
		}

		if fp.recvdMessageKey != "bar" {
			t.Errorf("expected 'bar' as message key, but was '%s' instead", fp.recvdMessageKey)
		}

		if !reflect.DeepEqual(msg.Key, mk) {
			t.Error("expected the message key to be reset on the message")
		}
	})

	t.Run("delegates to default hashPartitioner behaviour if there is a nil message key", func(t *testing.T) {
		t.Parallel()
		fp := newFakeHashPartitioner(false)
		fp.partitionToReturn = 0
		ob := NewOutboxPartitionerWithCustomPartitioner("product", fp)
		got, err := ob.Partition(&sarama.ProducerMessage{}, 2)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if got != 0 {
			t.Errorf("expected partition 0 but got %d", got)
		}
	})

	t.Run("key is used when partition key is empty", func(t *testing.T) {
		t.Parallel()
		fp := newFakeHashPartitioner(false)
		fp.partitionToReturn = 0
		ob := NewOutboxPartitionerWithCustomPartitioner("product", fp)
		msg := &sarama.ProducerMessage{Key: newMessageKey("foo", "")}

		got, err := ob.Partition(msg, 2)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if got != 0 {
			t.Errorf("expected partition 0 but got %d", got)
		}

		if fp.recvdMessageKey != "foo" {
			t.Errorf("expected message key on hashPartitioner to be 'foo', but was '%s' instead", fp.recvdMessageKey)
		}
	})

	t.Run("error is returned from hashPartitioner", func(t *testing.T) {
		t.Parallel()
		fp := newFakeHashPartitioner(true)
		ob := NewOutboxPartitionerWithCustomPartitioner("product", fp)
		msg := &sarama.ProducerMessage{Key: newMessageKey("foo", "bar")}

		_, err := ob.Partition(msg, 2)
		if err == nil {
			t.Error("expected an error but got nil")
		}
	})
}

func TestOutboxPartitioner_RequiresConsistency(t *testing.T) {
	req := OutboxPartitioner{}.RequiresConsistency()
	if !req {
		t.Error("expected OutboxPartitioner to require consistency, but it does not")
	}
}

func newFakeHashPartitioner(error bool) *fakeHashPartitioner {
	return &fakeHashPartitioner{
		returnError: error,
	}
}

type fakeHashPartitioner struct {
	recvdMessageKey    string
	recvdNumPartitions int32
	returnError        bool
	partitionToReturn  int32
}

func (fp *fakeHashPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key != nil {
		key, err := message.Key.Encode()
		if err != nil {
			return 0, err
		}
		fp.recvdMessageKey = string(key)
	}

	fp.recvdNumPartitions = numPartitions

	if fp.returnError {
		return 0, errors.New("oops")
	}
	return fp.partitionToReturn, nil
}

func (fp *fakeHashPartitioner) RequiresConsistency() bool {
	return false
}
