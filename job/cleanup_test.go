package job

import (
	"inviqa/kafka-outbox-relay/job/test"
	outboxtest "inviqa/kafka-outbox-relay/outbox/test"
	"net/http"
	"testing"
)

func TestNewCleanup(t *testing.T) {
	repo := outboxtest.NewMockRepository()
	cl := &http.Client{}

	j := newCleanup(repo, cl)
	if j == nil {
		t.Errorf("received nil instead of cleanup job")
	}
}

func TestNewCleanupWithDefaultClient(t *testing.T) {
	repo := outboxtest.NewMockRepository()

	j := newCleanupWithDefaultClient(repo)
	if j == nil {
		t.Errorf("received nil instead of cleanup job")
	}
}

func TestCleanup_Execute(t *testing.T) {
	repo := outboxtest.NewMockRepository()
	repo.SetDeletedRowsCount(100)
	cl := test.NewMockHttpClient()
	j := newCleanup(repo, cl)
	rows, err := j.Execute()

	if err != nil {
		t.Errorf("unexpected error received: %s", err)
	}

	if rows != 100 {
		t.Errorf("expected 100 affected rows, but got %d", rows)
	}

	if len(cl.SentReqs) > 0 {
		t.Errorf("unexpected call to sidecar proxy /quitquitquit")
	}
}

func TestCleanup_ExecuteWithSidecarProxyQuit(t *testing.T) {
	repo := outboxtest.NewMockRepository()
	repo.SetDeletedRowsCount(99)
	cl := test.NewMockHttpClient()
	j := newCleanup(repo, cl)
	j.EnableSideCarProxyQuit("http://localhost:9090")
	rows, err := j.Execute()

	if err != nil {
		t.Errorf("unexpected error received: %s", err)
	}

	if rows != 99 {
		t.Errorf("expected 99 affected rows, but got %d", rows)
	}

	if cl.SentReqs["http://localhost:9090/quitquitquit"] == false {
		t.Errorf("expected a call to sidecar proxy http://localhost:9090/quitquitquit")
	}
}

func TestCleanup_ExecuteWithRepoError(t *testing.T) {
	repo := outboxtest.NewMockRepository()
	repo.ReturnErrors()
	cl := test.NewMockHttpClient()
	j := newCleanup(repo, cl)
	_, err := j.Execute()

	if err == nil {
		t.Error("expected an error, but got nil")
	}

	if len(cl.SentReqs) > 0 {
		t.Errorf("unexpected call to sidecar proxy /quitquitquit")
	}
}

func TestCleanup_ExecuteWithHttpClientError(t *testing.T) {
	repo := outboxtest.NewMockRepository()
	cl := test.NewMockHttpClient()
	cl.ReturnErrors()
	j := newCleanup(repo, cl)
	j.EnableSideCarProxyQuit("http://localhost:15000/")
	_, err := j.Execute()

	if err == nil {
		t.Error("expected an error, but got nil")
	}
}
