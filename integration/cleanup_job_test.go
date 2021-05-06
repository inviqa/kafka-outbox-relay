// +build integration

package integration

import (
	"database/sql"
	"inviqa/kafka-outbox-relay/integration/http"
	"inviqa/kafka-outbox-relay/job"
	"inviqa/kafka-outbox-relay/outbox"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCleanupJobRemovesPublishedMessages(t *testing.T) {
	purgeOutboxTable()

	Convey("Given there are old messages in the outbox", t, func() {
		old := sql.NullTime{
			Time:  time.Now().Add(time.Duration(-2) * time.Hour),
			Valid: true,
		}
		recent := sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		}
		msg1 := &outbox.Message{
			PayloadJson:     []byte(`{"foo": "bar"}`),
			Topic:           "testProductUpdate",
			PushStartedAt:   old,
			PushCompletedAt: old,
		}
		msg2 := &outbox.Message{
			PayloadJson:     []byte(`{"foo": "bar"}`),
			Topic:           "testProductUpdate",
			PushStartedAt:   old,
			PushCompletedAt: old,
		}
		msg3 := &outbox.Message{
			PayloadJson:     []byte(`{"foo": "bar"}`),
			Topic:           "testProductUpdate",
			PushStartedAt:   recent,
			PushCompletedAt: recent,
		}
		insertOutboxMessages([]*outbox.Message{msg1, msg2, msg3})

		Convey("When we execute a cleanup of the outbox", func() {
			code := job.RunCleanup(publishedDeleter, cfg)

			Convey("Then the old messages should have been deleted", func() {
				So(code, ShouldEqual, 0)

				So(outboxMessageExists(msg1.Id), ShouldBeFalse)
				So(outboxMessageExists(msg2.Id), ShouldBeFalse)

				Convey("And the more recent messages should not have been deleted", func() {
					So(outboxMessageExists(msg3.Id), ShouldBeTrue)
				})
			})
		})
	})
}

func TestCleanupJobRemovesPublishedMessagesWithHugeNumberOfMessages(t *testing.T) {
	purgeOutboxTable()

	Convey("Given there are a huge amount of old messages in the outbox", t, func() {
		old := sql.NullTime{
			Time:  time.Now().Add(time.Duration(-2) * time.Hour),
			Valid: true,
		}

		var msgs []*outbox.Message
		for i := 0; i < 1000; i++ {
			msg := &outbox.Message{
				PayloadJson:     []byte(`{"foo": "bar"}`),
				Topic:           "testProductUpdate",
				PushStartedAt:   old,
				PushCompletedAt: old,
			}
			msgs = append(msgs, msg)
		}

		insertOutboxMessages(msgs)

		Convey("And there are also some more recent messages in the outbox", func() {
			recent := sql.NullTime{
				Time:  time.Now(),
				Valid: true,
			}
			msg1 := &outbox.Message{
				PayloadJson:     []byte(`{"foo": "bar"}`),
				Topic:           "testProductUpdate",
				PushStartedAt:   recent,
				PushCompletedAt: recent,
			}
			msg2 := &outbox.Message{
				PayloadJson:     []byte(`{"foo": "bar"}`),
				Topic:           "testProductUpdate",
				PushStartedAt:   recent,
				PushCompletedAt: recent,
			}

			insertOutboxMessages([]*outbox.Message{msg1, msg2})

			Convey("When we execute a cleanup of the outbox", func() {
				code := job.RunCleanup(publishedDeleter, cfg)

				Convey("Then the old messages should have been deleted", func() {
					So(code, ShouldEqual, 0)

					ok := true
					for _, m := range msgs {
						ok = !outboxMessageExists(m.Id) && ok
					}

					So(ok, ShouldBeTrue)

					Convey("And the more recent messages should not have been deleted", func() {
						So(outboxMessageExists(msg1.Id), ShouldBeTrue)
						So(outboxMessageExists(msg2.Id), ShouldBeTrue)
					})
				})
			})
		})
	})
}

func TestCleanupJobQuitsSidecarProxyWhenConfiguredToDoSo(t *testing.T) {
	purgeOutboxTable()
	http.Reset()

	Convey("Given there is an old message in the outbox", t, func() {
		old := sql.NullTime{
			Time:  time.Now().Add(time.Duration(-2) * time.Hour),
			Valid: true,
		}
		msg1 := &outbox.Message{
			PayloadJson:     []byte(`{"foo": "bar"}`),
			Topic:           "testProductUpdate",
			PushStartedAt:   old,
			PushCompletedAt: old,
		}
		insertOutboxMessages([]*outbox.Message{msg1})

		Convey("When we execute a cleanup of the outbox", func() {
			code := job.RunCleanup(publishedDeleter, cfg)

			Convey("Then the old messages should have been deleted", func() {
				So(code, ShouldEqual, 0)

				So(outboxMessageExists(msg1.Id), ShouldBeFalse)

				Convey("And a request to quit the sidecar proxy should have been sent via HTTP", func() {
					So(http.Recvd["/quitquitquit"], ShouldBeTrue)
				})
			})
		})
	})
}
