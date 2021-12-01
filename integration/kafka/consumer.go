//go:build integration
// +build integration

package kafka

import (
	"github.com/Shopify/sarama"
)

type ConsumerHandler struct {
	MessagesFound bool
	Consume       func(message *sarama.ConsumerMessage, c *ConsumerHandler)
}

func (c *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		c.Consume(message, c)
		session.MarkMessage(message, "")
	}

	return nil
}

func (c *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}
