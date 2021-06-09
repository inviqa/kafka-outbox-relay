// +build integration

package integration

import (
	"fmt"
	"testing"
	"time"

	testkafka "inviqa/kafka-outbox-relay/integration/kafka"
	"inviqa/kafka-outbox-relay/outbox"

	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

func TestMessagesCanBeRetriedIfKafkaPublishFails_WhenPushAttemptsAreLessThanMaximum(t *testing.T) {
	Convey(fmt.Sprintf("Given I have a %s outbox table", cfg.DBDriver), t, func() {
		purgeOutboxTable()
		Convey("Given there are messages in the outbox to be processed", t, func() {
			msg1 := &outbox.Message{
				PayloadJson:    []byte(`{"foo": "bar"}`),
				PayloadHeaders: []byte(`{"x-event-id": 1}`),
				PushAttempts:   3,
				Topic:          "testProductUpdate",
			}
			msg2 := &outbox.Message{
				PayloadJson:    []byte(`{"foo": "baz"}`),
				PayloadHeaders: []byte(`{"x-event-id": 2}`),
				PushAttempts:   1,
				Topic:          "testProductUpdate",
			}

			insertOutboxMessages([]*outbox.Message{msg1, msg2})

			Convey("When the outbox relay service polls the database", func() {
				time.Sleep(time.Millisecond * 500)
				Convey("Then a batch of messages should have been sent to Kafka", func() {
					cons := consumeFromKafkaUntilMessagesReceived([]testkafka.MessageExpectation{
						{Msg: msg1, Headers: []*sarama.RecordHeader{{Key: []byte("x-event-id"), Value: []byte("1")}}},
						{Msg: msg2, Headers: []*sarama.RecordHeader{{Key: []byte("x-event-id"), Value: []byte("2")}}},
					})
					So(cons.MessagesFound, ShouldBeTrue)
					Convey("And the messages should have been marked as completed", func() {
						for _, m := range []*outbox.Message{msg1, msg2} {
							actual := getOutboxMessage(m.Id)
							So(actual.Errored, ShouldBeFalse)
							So(actual.ErrorReason, ShouldBeNil)
							So(actual.PushCompletedAt.Valid, ShouldBeTrue)
							So(actual.PushCompletedAt.Time.IsZero(), ShouldBeFalse)
							So(actual.PushAttempts, ShouldEqual, m.PushAttempts+1)
						}
					})
				})
			})
		})
	})
}
