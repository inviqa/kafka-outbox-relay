package prometheus

import (
	"context"
	"testing"
	"time"

	"inviqa/kafka-outbox-relay/outbox/test"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserveTotalSize(t *testing.T) {
	repo1 := test.NewMockRepository()
	repo1.SetTotalSize(76)
	repo2 := test.NewMockRepository()
	repo2.SetTotalSize(10)

	ctx, cancel := context.WithCancel(context.Background())
	go ObserveTotalSize([]Sizer{repo1, repo2}, ctx)
	time.Sleep(time.Millisecond * 100)
	cancel()

	actual := testutil.ToFloat64(outboxTotalSize)
	if actual != 86.00 {
		t.Errorf("expected outboxTotalSize to be 76.000000, but got %f", actual)
	}
}

func TestObserveTotalSize_WithRepositoryError(t *testing.T) {
	outboxTotalSize.Set(0.0)
	repo := test.NewMockRepository()
	repo.ReturnErrors()

	ctx, cancel := context.WithCancel(context.Background())
	go ObserveTotalSize([]Sizer{repo}, ctx)
	time.Sleep(time.Millisecond * 100)
	cancel()

	actual := testutil.ToFloat64(outboxTotalSize)
	if actual != 0.00 {
		t.Errorf("expected outboxTotalSize to be 0.000000, but got %f", actual)
	}
}
