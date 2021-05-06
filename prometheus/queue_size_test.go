package prometheus

import (
	"context"
	"inviqa/kafka-outbox-relay/outbox/test"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserveQueueSize(t *testing.T) {
	repo := test.NewMockRepository()
	repo.SetQueueSize(32)

	ctx, cancel := context.WithCancel(context.Background())
	go ObserveQueueSize(repo, ctx)
	time.Sleep(time.Millisecond * 100)
	cancel()

	actual := testutil.ToFloat64(outboxQueueSize)
	if actual != 32.00 {
		t.Errorf("expected outboxQueueSize to be 32.000000, but got %f", actual)
	}
}

func TestObserveQueueSize_WithRepositoryError(t *testing.T) {
	outboxQueueSize.Set(0.0)
	repo := test.NewMockRepository()
	repo.ReturnErrors()

	ctx, cancel := context.WithCancel(context.Background())
	go ObserveQueueSize(repo, ctx)
	time.Sleep(time.Millisecond * 100)
	cancel()

	actual := testutil.ToFloat64(outboxQueueSize)
	if actual != 0.00 {
		t.Errorf("expected outboxQueueSize to be 0.000000, but got %f", actual)
	}
}
