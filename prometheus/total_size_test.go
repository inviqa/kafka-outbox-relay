package prometheus

import (
	"context"
	"inviqa/kafka-outbox-relay/outbox/test"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserveTotalSize(t *testing.T) {
	repo := test.NewMockRepository()
	repo.SetTotalSize(76)

	ctx, cancel := context.WithCancel(context.Background())
	go ObserveTotalSize(repo, ctx)
	time.Sleep(time.Millisecond * 100)
	cancel()

	actual := testutil.ToFloat64(outboxTotalSize)
	if actual != 76.00 {
		t.Errorf("expected outboxTotalSize to be 76.000000, but got %f", actual)
	}
}

func TestObserveTotalSize_WithRepositoryError(t *testing.T) {
	outboxTotalSize.Set(0.0)
	repo := test.NewMockRepository()
	repo.ReturnErrors()

	ctx, cancel := context.WithCancel(context.Background())
	go ObserveTotalSize(repo, ctx)
	time.Sleep(time.Millisecond * 100)
	cancel()

	actual := testutil.ToFloat64(outboxTotalSize)
	if actual != 0.00 {
		t.Errorf("expected outboxTotalSize to be 0.000000, but got %f", actual)
	}
}
