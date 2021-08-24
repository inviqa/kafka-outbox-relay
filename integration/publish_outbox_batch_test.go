// +build integration

package integration

import (
	"errors"
	"testing"

	testkafka "inviqa/kafka-outbox-relay/integration/kafka"
	"inviqa/kafka-outbox-relay/outbox"

	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"
)

func TestPublishOutboxBatchSuccessfullyPublishesToKafka(t *testing.T) {
	purgeOutboxTable()

	Convey("Given there are messages in the outbox to be processed", t, func() {
		msg1 := &outbox.Message{
			PayloadJson:    []byte(`{"foo": "bar"}`),
			PayloadHeaders: []byte(`{"x-event-id": 1}`),
			Topic:          "testProductUpdate",
		}
		msg2 := &outbox.Message{
			PayloadJson:    []byte(`{"foo": "baz"}`),
			PayloadHeaders: []byte(`{"x-event-id": 2}`),
			Topic:          "testProductUpdate",
		}
		msg3 := &outbox.Message{
			PayloadJson:    []byte(`{"foo": "buzz"}`),
			PayloadHeaders: []byte(`{"x-event-id": 3}`),
			Topic:          "testProductUpdate",
		}

		insertOutboxMessages([]*outbox.Message{msg1, msg2, msg3})

		Convey("When the outbox relay service polls the database", func() {
			pollForMessages(1)
			Convey("Then a batch of messages should have been sent to Kafka", func() {
				cons := consumeFromKafkaUntilMessagesReceived([]testkafka.MessageExpectation{
					{Msg: msg1, Headers: []*sarama.RecordHeader{{Key: []byte("x-event-id"), Value: []byte("1")}}},
					{Msg: msg2, Headers: []*sarama.RecordHeader{{Key: []byte("x-event-id"), Value: []byte("2")}}},
					{Msg: msg3, Headers: []*sarama.RecordHeader{{Key: []byte("x-event-id"), Value: []byte("3")}}},
				})
				So(cons.MessagesFound, ShouldBeTrue)
				Convey("And the messages should have been marked as completed", func() {
					for _, m := range []*outbox.Message{msg1, msg2, msg3} {
						actual := getOutboxMessage(m.Id)
						So(actual.Errored, ShouldBeFalse)
						So(actual.ErrorReason, ShouldBeNil)
						So(actual.PushCompletedAt.Valid, ShouldBeTrue)
						So(actual.PushCompletedAt.Time.IsZero(), ShouldBeFalse)
						So(actual.PushAttempts, ShouldEqual, 1)
					}
				})
			})
		})
	})
}

func TestPublishOutboxBatchCorrectlyMarksFailedMessagesAsErrored(t *testing.T) {
	purgeOutboxTable()
	Convey("Given there are messages in the outbox to be processed", t, func() {
		msg1 := &outbox.Message{
			PayloadJson:    []byte(`{"foo": "bar"}`),
			PayloadHeaders: []byte(`{"x-event-id": 1}`),
			Topic:          "testProductUpdate",
			PushAttempts:   2,
		}
		msg2 := &outbox.Message{
			PayloadJson:    []byte(`{"foo": "baz"}`),
			PayloadHeaders: []byte(`{"x-event-id": 2}`),
			Topic:          "testProductUpdate",
			PushAttempts:   2,
		}
		msg3 := &outbox.Message{
			PayloadJson:    []byte(`{"foo": "buzz"}`),
			PayloadHeaders: []byte(`{"x-event-id": 3}`),
			Topic:          "testProductUpdate",
			PushAttempts:   2,
		}

		returnErrorFromSyncProducerForMessage(string(msg1.PayloadJson), errors.New("producer error"))
		returnErrorFromSyncProducerForMessage(string(msg3.PayloadJson), errors.New("producer error"))

		insertOutboxMessages([]*outbox.Message{msg1, msg2, msg3})

		Convey("When the outbox relay service polls the database", func() {
			pollForMessages(1)
			Convey("Then the batch of messages should have been sent to Kafka", func() {
				cons := consumeFromKafkaUntilMessagesReceived([]testkafka.MessageExpectation{
					{Msg: msg2, Headers: []*sarama.RecordHeader{{Key: []byte("x-event-id"), Value: []byte("2")}}},
				})
				So(cons.MessagesFound, ShouldBeTrue)
				Convey("And the successful messages should have been marked as completed", func() {
					actualMsg2 := getOutboxMessage(msg2.Id)
					So(actualMsg2.Errored, ShouldBeFalse)
					So(actualMsg2.ErrorReason, ShouldBeNil)
					So(actualMsg2.PushCompletedAt.Valid, ShouldBeTrue)
					So(actualMsg2.PushCompletedAt.Time.IsZero(), ShouldBeFalse)
					So(actualMsg2.PushAttempts, ShouldEqual, 3)

					Convey("And the errored messages should have been marked as failed", func() {
						for _, m := range []*outbox.Message{msg1, msg3} {
							actual := getOutboxMessage(m.Id)
							So(actual.Errored, ShouldBeTrue)
							So(actual.ErrorReason, ShouldNotBeNil)
							So(actual.ErrorReason.Error(), ShouldEqual, "error producing message in Kafka: producer error")
							So(actual.PushCompletedAt.Time.IsZero(), ShouldBeTrue)
							So(actual.PushCompletedAt.Valid, ShouldBeFalse)
							So(actual.PushAttempts, ShouldBeGreaterThanOrEqualTo, 3) // See integration/helper_test.go:153
						}
					})
				})
			})
		})
	})
}
