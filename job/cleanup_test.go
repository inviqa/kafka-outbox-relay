package job

import (
	"context"
	"net/http"
	"testing"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/job/test"
	"inviqa/kafka-outbox-relay/outbox/data"
	outboxtest "inviqa/kafka-outbox-relay/outbox/test"
)

func TestNewCleanup(t *testing.T) {
	cl := &http.Client{}

	if newCleanup(cl) == nil {
		t.Errorf("received nil instead of cleanup job")
	}
}

func TestNewCleanupWithDefaultClient(t *testing.T) {
	j := newCleanupWithDefaults(data.DB{}, &config.Config{})
	if j == nil {
		t.Errorf("received nil instead of cleanup job")
	}
}

func TestCleanup_Execute(t *testing.T) {
	ctx := context.Background()
	repo := outboxtest.NewMockRepository()
	repo.SetDeletedRowsCount(100)
	cl := test.NewMockHttpClient()
	j := newTestCleanup(cl, repo)

	if err := j.Execute(ctx); err != nil {
		t.Errorf("unexpected error received: %s", err)
	}

	if len(cl.SentReqs) > 0 {
		t.Errorf("unexpected call to sidecar proxy /quitquitquit")
	}
}

func TestCleanup_ExecuteWithSidecarProxyQuit(t *testing.T) {
	ctx := context.Background()
	repo := outboxtest.NewMockRepository()
	repo.SetDeletedRowsCount(99)
	cl := test.NewMockHttpClient()
	j := newTestCleanup(cl, repo)
	j.EnableSideCarProxyQuit("http://localhost:9090")

	if err := j.Execute(ctx); err != nil {
		t.Errorf("unexpected error received: %s", err)
	}

	if cl.SentReqs["http://localhost:9090/quitquitquit"] == false {
		t.Errorf("expected a call to sidecar proxy http://localhost:9090/quitquitquit")
	}
}

func TestCleanup_ExecuteWithRepoError(t *testing.T) {
	ctx := context.Background()
	repo := outboxtest.NewMockRepository()
	repo.ReturnErrors()
	cl := test.NewMockHttpClient()
	j := newTestCleanup(cl, repo)

	if err := j.Execute(ctx); err == nil {
		t.Error("expected an error, but got nil")
	}

	if len(cl.SentReqs) > 0 {
		t.Errorf("unexpected call to sidecar proxy /quitquitquit")
	}
}

func TestCleanup_ExecuteWithHttpClientError(t *testing.T) {
	ctx := context.Background()
	repo := outboxtest.NewMockRepository()
	cl := test.NewMockHttpClient()
	cl.ReturnErrors()
	j := newTestCleanup(cl, repo)
	j.EnableSideCarProxyQuit("http://localhost:15000/")

	if err := j.Execute(ctx); err == nil {
		t.Error("expected an error, but got nil")
	}
}

func newCleanup(cl httpPoster) *cleanup {
	return &cleanup{
		SidecarQuitter: SidecarQuitter{
			Client: cl,
		},
	}
}

func newTestCleanup(cl *test.MockHttpClient, repo *outboxtest.MockRepository) *cleanup {
	return &cleanup{
		SidecarQuitter: SidecarQuitter{
			Client: cl,
		},
		deleterFactory: testDeleterFactory(repo),
	}
}

func testDeleterFactory(mock *outboxtest.MockRepository) func() publishedDeleter {
	return func() publishedDeleter {
		return mock
	}
}
