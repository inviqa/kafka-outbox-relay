package job

import (
	"net/http"
	"testing"

	"inviqa/kafka-outbox-relay/job/test"
	outboxtest "inviqa/kafka-outbox-relay/outbox/test"
)

func TestNewCleanup(t *testing.T) {
	cl := &http.Client{}

	if newCleanup(cl) == nil {
		t.Errorf("received nil instead of cleanup job")
	}
}

func TestNewCleanupWithDefaultClient(t *testing.T) {
	j := newCleanupWithDefaultClient()
	if j == nil {
		t.Errorf("received nil instead of cleanup job")
	}
}

func TestCleanup_Execute(t *testing.T) {
	repo := outboxtest.NewMockRepository()
	repo.SetDeletedRowsCount(100)
	cl := test.NewMockHttpClient()
	j := newTestCleanup(cl, repo)

	if err := j.Execute(); err != nil {
		t.Errorf("unexpected error received: %s", err)
	}

	if len(cl.SentReqs) > 0 {
		t.Errorf("unexpected call to sidecar proxy /quitquitquit")
	}
}

func TestCleanup_ExecuteWithSidecarProxyQuit(t *testing.T) {
	repo := outboxtest.NewMockRepository()
	repo.SetDeletedRowsCount(99)
	cl := test.NewMockHttpClient()
	j := newTestCleanup(cl, repo)
	j.EnableSideCarProxyQuit("http://localhost:9090")

	if err := j.Execute(); err != nil {
		t.Errorf("unexpected error received: %s", err)
	}

	if cl.SentReqs["http://localhost:9090/quitquitquit"] == false {
		t.Errorf("expected a call to sidecar proxy http://localhost:9090/quitquitquit")
	}
}

func TestCleanup_ExecuteWithRepoError(t *testing.T) {
	repo := outboxtest.NewMockRepository()
	repo.ReturnErrors()
	cl := test.NewMockHttpClient()
	j := newTestCleanup(cl, repo)

	if err := j.Execute(); err == nil {
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
	j := newTestCleanup(cl, repo)
	j.EnableSideCarProxyQuit("http://localhost:15000/")

	if err := j.Execute(); err == nil {
		t.Error("expected an error, but got nil")
	}
}

func newTestCleanup(cl *test.MockHttpClient, repo *outboxtest.MockRepository) *cleanup {
	j := newCleanup(cl)
	j.deleterFactory = testDeleterFactory(repo)
	return j
}

func testDeleterFactory(mock *outboxtest.MockRepository) func() publishedDeleter {
	return func() publishedDeleter {
		return mock
	}
}
