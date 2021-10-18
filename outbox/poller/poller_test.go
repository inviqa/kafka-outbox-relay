package poller

import (
	"context"
	"reflect"
	"runtime"
	"testing"
	"time"

	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/test"

	"github.com/google/uuid"
)

func TestNew(t *testing.T) {
	repo := test.NewMockRepository()
	ch := make(chan *outbox.Batch)

	if nil == New(repo, ch) {
		t.Errorf("received nil from New()")
	}
}

func Test_Poller_Poll(t *testing.T) {
	ch := make(chan *outbox.Batch, 2)

	b1 := &outbox.Batch{Id: uuid.New(), Messages: []*outbox.Message{{Id: 1}}}
	b2 := &outbox.Batch{Id: uuid.New(), Messages: []*outbox.Message{{Id: 2}}}

	repoWithBatches := test.NewMockRepository()
	repoWithBatches.AddBatch(b1)
	repoWithBatches.AddBatch(b2)

	t.Run("it polls for events and sends them for processing", func(t *testing.T) {
		p := New(repoWithBatches, ch)
		go p.Poll(context.Background(), time.Millisecond*10)

		readFromChannelUntilBatchReceived(b1, ch, t)
		readFromChannelUntilBatchReceived(b2, ch, t)
	})

	t.Run("it sleeps after repository error", func(t *testing.T) {
		repo := test.NewMockRepository()
		repo.ReturnErrors()

		ctx, cancel := context.WithCancel(context.Background())
		p := New(repo, ch)
		go p.Poll(ctx, time.Second*200)

		time.Sleep(time.Millisecond * 100)
		cancel()

		if repo.GetBatchCallCount() > 1 {
			t.Errorf("expected the outbox Poll func to sleep after GetBatch() returns an error")
		}
	})

	t.Run("it sleeps when no events were found", func(t *testing.T) {
		repo := test.NewMockRepository()
		repo.ReturnNoEventsError()

		ctx, cancel := context.WithCancel(context.Background())
		p := New(repo, ch)
		go p.Poll(ctx, time.Second*200)

		time.Sleep(time.Millisecond * 100)
		cancel()

		if repo.GetBatchCallCount() > 1 {
			t.Errorf("expected the outbox Poll func to sleep after GetBatch() returns no events")
		}
	})

	t.Run("it stops goroutine when context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		p := New(repoWithBatches, ch)
		go p.Poll(ctx, time.Millisecond*10)

		routines := runtime.NumGoroutine()
		cancel()
		time.Sleep(time.Millisecond * 50)
		routinesAfterCancel := runtime.NumGoroutine()

		if (routines - 1) != routinesAfterCancel {
			t.Errorf(
				"after context was cancelled the number of goroutines should have decreased by 1 (before context.Cancel: %d, after cancel: %d)",
				routines,
				routinesAfterCancel,
			)
		}
	})
}

func readFromChannelUntilBatchReceived(b *outbox.Batch, ch chan *outbox.Batch, t *testing.T) {
	select {
	case actual := <-ch:
		if !reflect.DeepEqual(actual, b) {
			t.Errorf("received wrong batch, got ID %s, but wanted ID %s", actual.Id, b.Id)
		}
		break
	case _ = <-time.After(time.Millisecond * 50):
		t.Errorf("expected batch ID %s to be received within 50ms, but was not", b.Id)
	}
}
