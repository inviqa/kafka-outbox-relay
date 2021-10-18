// +build integration

package integration

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"

	testkafka "inviqa/kafka-outbox-relay/integration/kafka"
	"inviqa/kafka-outbox-relay/outbox"
)

func TestAbandonedMessagesAreCorrectlyPublishedAgain(t *testing.T) {
	Convey(fmt.Sprintf("Given I have a %s outbox table", cfg.DBDriver), t, func() {
		purgeOutboxTable()

		Convey("And there are some abandoned messages in the outbox", func() {
			batchId := uuid.New()
			beforeStaleThreshold := sql.NullTime{
				Time:  time.Now().In(time.UTC).Add(time.Duration(-1) * time.Hour),
				Valid: true,
			}
			msg1 := &outbox.Message{
				BatchId:       &batchId,
				PayloadJson:   []byte(`{"foo": "bar"}`),
				Topic:         "testProductUpdate",
				PushStartedAt: beforeStaleThreshold,
				PushAttempts:  1,
			}
			msg2 := &outbox.Message{
				BatchId:       &batchId,
				PayloadJson:   []byte(`{"foo": "bar"}`),
				Topic:         "testProductUpdate",
				PushStartedAt: beforeStaleThreshold,
				PushAttempts:  1,
			}
			insertOutboxMessages([]*outbox.Message{msg1, msg2})

			Convey("When the outbox relay service polls the database", func() {
				pollForMessages(1)
				Convey("Then the batch of messages should have been sent to Kafka", func() {
					cons := consumeFromKafkaUntilMessagesReceived([]testkafka.MessageExpectation{
						{Msg: msg1, Headers: []*sarama.RecordHeader{}},
						{Msg: msg2, Headers: []*sarama.RecordHeader{}},
					})
					So(cons.MessagesFound, ShouldBeTrue)
					Convey("And the successful messages should have been marked as completed", func() {
						for _, msg := range []*outbox.Message{msg1, msg2} {
							actualMsg := getOutboxMessage(msg.Id)
							So(actualMsg.BatchId.String(), ShouldNotEqual, batchId.String())
							So(actualMsg.Errored, ShouldBeFalse)
							So(actualMsg.ErrorReason, ShouldBeNil)
							So(actualMsg.PushCompletedAt.Valid, ShouldBeTrue)
							So(actualMsg.PushCompletedAt.Time.IsZero(), ShouldBeFalse)
							So(actualMsg.PushAttempts, ShouldEqual, 2)
						}
					})
				})
			})
		})
	})
}
