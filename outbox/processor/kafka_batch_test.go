package processor

import (
	"context"
	"runtime"
	"testing"
	"time"

	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/processor/test"
	otest "inviqa/kafka-outbox-relay/outbox/test"

	"github.com/go-test/deep"
	"github.com/google/uuid"
)

func TestNewBatchProcessor(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	repo := otest.NewMockRepository()
	pub := test.NewMockPublisher()

	exp := KafkaBatchProcessor{
		repo:      repo,
		publisher: pub,
		nrApp:     nil,
	}

	if diff := deep.Equal(exp, NewBatchProcessor(repo, pub, nil)); diff != nil {
		t.Error(diff)
	}
}

func TestKafkaBatchProcessor_ListenAndProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := otest.NewMockRepository()
	pub := test.NewMockPublisher()
	ch := make(chan *outbox.Batch)

	proc := NewBatchProcessor(repo, pub, nil)
	go proc.ListenAndProcess(ctx, ch)

	b1 := &outbox.Batch{
		Id: uuid.New(),
		Messages: []*outbox.Message{
			{
				Id:    1,
				Topic: "foo",
			},
			{
				Id:    2,
				Topic: "foo",
			},
		},
	}
	b2 := &outbox.Batch{
		Id: uuid.New(),
		Messages: []*outbox.Message{
			{
				Id:    3,
				Topic: "foo",
			},
			{
				Id:    4,
				Topic: "foo",
			},
		},
	}

	ch <- b1
	ch <- b2

	time.Sleep(time.Millisecond * 1)

	if !pub.MessageWasPublished(b1.Messages[0]) || !pub.MessageWasPublished(b1.Messages[1]) {
		t.Errorf("messages from the first batch were not published")
	}

	if !pub.MessageWasPublished(b2.Messages[0]) || !pub.MessageWasPublished(b2.Messages[1]) {
		t.Errorf("messages from the second batch were not published")
	}

	if !repo.BatchWasCommitted(b1) {
		t.Error("first batch was not committed")
	}

	if !repo.BatchWasCommitted(b2) {
		t.Error("second batch was not committed")
	}
}

func TestKafkaBatchProcessor_ListenAndProcessWithPublishError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := otest.NewMockRepository()
	pub := test.NewMockPublisher()
	ch := make(chan *outbox.Batch)

	proc := NewBatchProcessor(repo, pub, nil)
	go proc.ListenAndProcess(ctx, ch)

	b1 := &outbox.Batch{
		Id: uuid.New(),
		Messages: []*outbox.Message{
			{
				Id:    1,
				Topic: "foo",
			},
		},
	}
	b2 := &outbox.Batch{
		Id: uuid.New(),
		Messages: []*outbox.Message{
			{
				Id:    4,
				Topic: "foo",
			},
		},
	}

	pub.ErrorForMessage(b1.Messages[0])

	ch <- b1
	ch <- b2

	time.Sleep(time.Millisecond * 1)

	if !pub.MessageWasPublished(b2.Messages[0]) {
		t.Errorf("messages from the second batch were not published")
	}

	if !repo.BatchWasCommitted(b1) {
		t.Error("first batch was not committed")
	}

	if !repo.BatchWasCommitted(b2) {
		t.Error("second batch was not committed")
	}

	committedB1 := repo.GetCommittedBatch(b1)
	if committedB1.Messages[0].ErrorReason == nil {
		t.Errorf("first committed batch's message was not marked with an error")
	}
}

func TestKafkaBatchProcessor_ListenAndProcessIgnoresMessagesWithNoTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := otest.NewMockRepository()
	pub := test.NewMockPublisher()
	ch := make(chan *outbox.Batch)

	proc := NewBatchProcessor(repo, pub, nil)
	go proc.ListenAndProcess(ctx, ch)

	b1 := &outbox.Batch{
		Id: uuid.New(),
		Messages: []*outbox.Message{
			{
				Id: 1,
			},
			{
				Id:    4,
				Topic: "foo",
			},
		},
	}

	ch <- b1

	time.Sleep(time.Millisecond * 1)

	if pub.MessageWasPublished(b1.Messages[0]) {
		t.Errorf("a message without a topic was published to kafka")
	}

	if !pub.MessageWasPublished(b1.Messages[1]) {
		t.Errorf("message with a topic was not published to kafka as expected")
	}

	if !repo.BatchWasCommitted(b1) {
		t.Error("first batch was not committed")
	}

	committedB1 := repo.GetCommittedBatch(b1)
	if committedB1.Messages[0].ErrorReason == nil {
		t.Errorf("first committed batch's message was not marked with an error")
	}
}

func TestKafkaBatchProcessor_ListenAndProcessWithEmptyBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := otest.NewMockRepository()
	pub := test.NewMockPublisher()
	ch := make(chan *outbox.Batch)

	proc := NewBatchProcessor(repo, pub, nil)
	go proc.ListenAndProcess(ctx, ch)

	b1 := &outbox.Batch{
		Id:       uuid.New(),
		Messages: []*outbox.Message{},
	}

	ch <- b1

	time.Sleep(time.Millisecond * 1)

	if repo.BatchWasCommitted(b1) {
		t.Error("empty batch was committed when it should not have been")
	}
}

func TestKafkaBatchProcessor_ListenAndProcessWithNilBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := otest.NewMockRepository()
	pub := test.NewMockPublisher()
	ch := make(chan *outbox.Batch)

	proc := NewBatchProcessor(repo, pub, nil)
	go proc.ListenAndProcess(ctx, ch)

	ch <- nil

	time.Sleep(time.Millisecond * 1)
}

func TestKafkaBatchProcessor_ListenAndProcessTerminatesWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repo := otest.NewMockRepository()
	pub := test.NewMockPublisher()
	ch := make(chan *outbox.Batch)

	proc := NewBatchProcessor(repo, pub, nil)
	go proc.ListenAndProcess(ctx, ch)

	routines := runtime.NumGoroutine()
	cancel()

	var checks uint
	for {
		routinesAfterCancel := runtime.NumGoroutine()
		if routinesAfterCancel < routines {
			break
		}
		if checks == 10 {
			t.Errorf(
				"after the context was cancelled the number of goroutines should have decreased by 1 (before context.Cancel: %d, after cancel: %d)",
				routines,
				routinesAfterCancel,
			)
		}
		checks++
		time.Sleep(time.Millisecond * 10)
	}
}
