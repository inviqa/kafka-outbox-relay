package kafka

import (
	"crypto/tls"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

func NewSaramaConfig(tlsEnable bool, tlsSkipVerify bool) *sarama.Config {
	cfg := sarama.NewConfig()

	host, _ := os.Hostname()

	cfg.ClientID = host
	cfg.Version = sarama.V2_4_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.Compression = sarama.CompressionGZIP
	cfg.Metadata.Retry.Max = 10
	cfg.Metadata.Retry.Backoff = 2 * time.Second
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	if tlsEnable {
		cfg.Net.TLS.Enable = true
		// #nosec G402
		// we suppress this in gosec because it believes that InsecureSkipVerify is true, but it depends on the parameter
		// value passed into this func, which is dependent on environment configuration
		cfg.Net.TLS.Config = &tls.Config{InsecureSkipVerify: tlsSkipVerify}
	}

	return cfg
}
