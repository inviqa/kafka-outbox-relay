package kafka

import (
	"errors"
	"testing"

	"inviqa/kafka-outbox-relay/kafka/test"
	"inviqa/kafka-outbox-relay/outbox"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/go-test/deep"
)

func TestNewPublisherWithProducer(t *testing.T) {
	deep.CompareUnexportedFields = true
	deep.MaxDepth = 2
	defer func() {
		deep.CompareUnexportedFields = false
		deep.MaxDepth = 10
	}()

	prod := mocks.NewSyncProducer(t, NewSaramaConfig(false, false))
	exp := Publisher{
		producer: prod,
	}

	if diff := deep.Equal(exp, NewPublisherWithProducer(prod)); diff != nil {
		t.Error(diff)
	}
}

func TestPublisher_PublishMessage(t *testing.T) {
	prod := test.NewMockSyncProducer()
	pub := NewPublisherWithProducer(prod)

	msg := &outbox.Message{
		Id:             1,
		PayloadJson:    []byte(`{"payload"}`),
		PayloadHeaders: []byte(`{"x-event-id":"id"}`),
		Topic:          "productUpdate",
	}

	err := pub.PublishMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	exp := &sarama.ProducerMessage{
		Topic: "productUpdate",
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("x-event-id"),
				Value: []byte("id"),
			},
		},
		Value: sarama.ByteEncoder([]byte(`{"payload"}`)),
	}

	if err := prod.MessageWasProduced("productUpdate", exp); err != nil {
		t.Error(err)
	}
}

func TestPublisher_PublishMessageWithNilHeaders(t *testing.T) {
	prod := test.NewMockSyncProducer()
	pub := NewPublisherWithProducer(prod)

	msg := &outbox.Message{
		Id:          1,
		PayloadJson: []byte(`{"payload"}`),
		Topic:       "productUpdate",
	}

	err := pub.PublishMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	exp := &sarama.ProducerMessage{
		Topic:   "productUpdate",
		Headers: []sarama.RecordHeader{},
		Value:   sarama.ByteEncoder([]byte(`{"payload"}`)),
	}

	if err := prod.MessageWasProduced("productUpdate", exp); err != nil {
		t.Error(err)
	}
}

func TestPublisher_PublishMessageWithEmptyHeaders(t *testing.T) {
	cases := []string{"", "{}"}

	prod := test.NewMockSyncProducer()
	pub := NewPublisherWithProducer(prod)

	for _, val := range cases {
		msg := &outbox.Message{
			Id:             1,
			PayloadHeaders: []byte(val),
			PayloadJson:    []byte(`{"payload"}`),
			Topic:          "productUpdate",
		}

		err := pub.PublishMessage(msg)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		exp := &sarama.ProducerMessage{
			Topic:   "productUpdate",
			Headers: []sarama.RecordHeader{},
			Value:   sarama.ByteEncoder([]byte(`{"payload"}`)),
		}

		if err := prod.MessageWasProduced("productUpdate", exp); err != nil {
			t.Error(err)
		}
	}
}

func TestPublisher_PublishMessageWithIntHeaderValue(t *testing.T) {
	prod := test.NewMockSyncProducer()
	pub := NewPublisherWithProducer(prod)

	msg := &outbox.Message{
		Id:             1,
		PayloadJson:    []byte(`{"payload"}`),
		PayloadHeaders: []byte(`{"foo":1}`),
		Topic:          "productUpdate",
	}

	err := pub.PublishMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	exp := &sarama.ProducerMessage{
		Topic: "productUpdate",
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("foo"),
				Value: []byte("1"),
			},
		},
		Value: sarama.ByteEncoder([]byte(`{"payload"}`)),
	}

	if err := prod.MessageWasProduced("productUpdate", exp); err != nil {
		t.Error(err)
	}
}

func TestPublisher_PublishMessageWithHeadersUnmarshalError(t *testing.T) {
	prod := test.NewMockSyncProducer()
	pub := NewPublisherWithProducer(prod)

	msg := &outbox.Message{
		Id:             1,
		PayloadJson:    []byte(`{"payload"}`),
		PayloadHeaders: []byte(`{"x-}`),
		Topic:          "productUpdate",
	}

	err := pub.PublishMessage(msg)
	if err == nil {
		t.Error("expected an error but got nil")
	}
}

func TestPublisher_PublishMessageWithSendError(t *testing.T) {
	prod := mocks.NewSyncProducer(t, NewSaramaConfig(false, false))
	pub := NewPublisherWithProducer(prod)

	prod.ExpectSendMessageAndFail(errors.New("oops"))

	msg := &outbox.Message{
		Id:             2,
		PayloadJson:    []byte(`{"payload"}`),
		PayloadHeaders: []byte(`{"x-event-id":"id","foo":"bar"}`),
		Topic:          "productUpdate",
	}

	err := pub.PublishMessage(msg)
	if err == nil {
		t.Error("expected an error but got nil")
	}
}
