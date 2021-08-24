package kafka

import (
	"os"
	"testing"

	"github.com/Shopify/sarama"
)

func TestNewSaramaConfig(t *testing.T) {
	cfg := NewSaramaConfig(true, false)
	if nil == cfg {
		t.Fatal("expected config, but got nil")
	}

	expClientId, _ := os.Hostname()
	if cfg.ClientID != expClientId {
		t.Errorf("expected ClientID to be '%s', but got '%s'", expClientId, cfg.ClientID)
	}

	if cfg.Producer.Compression != sarama.CompressionGZIP {
		t.Errorf("expected GZIP compression to be enabled on producer")
	}

	if !cfg.Net.TLS.Enable {
		t.Error("expected TLS to be enabled")
	}

	if cfg.Net.TLS.Config != nil && cfg.Net.TLS.Config.InsecureSkipVerify {
		t.Error("expected TLS verification to be enabled")
	}

	partitioner := cfg.Producer.Partitioner("foo")
	if _, ok := partitioner.(OutboxPartitioner); !ok {
		t.Error("expected kafka.OutboxPartitioner, but did not get one")
	}
}
