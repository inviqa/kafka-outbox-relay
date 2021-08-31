package kafka

import (
	"testing"
)

func TestMessageKey_KeyForPartitioning(t *testing.T) {
	t.Run("partition key set", func(t *testing.T) {
		got := MessageKey{PartitionKey: "foo"}.KeyForPartitioning()
		if got != "foo" {
			t.Errorf("expected 'foo', got '%s'", got)
		}
	})

	t.Run("partition key not set", func(t *testing.T) {
		got := MessageKey{Key: "baz"}.KeyForPartitioning()
		if got != "baz" {
			t.Errorf("expected 'baz', got '%s'", got)
		}
	})
}
