package newrelic

import (
	"context"
	"reflect"
	"testing"

	"github.com/newrelic/go-agent/v3/newrelic"
)

func TestContextWithTxn(t *testing.T) {
	app, _ := newrelic.NewApplication(newrelic.ConfigEnabled(false))
	got, txn := ContextWithTxn(context.Background(), "foo", app)

	if txn == nil {
		t.Error("expected a *newrelic.Transaction, but got nil")
	}

	if ctxTxn := newrelic.FromContext(got); !reflect.DeepEqual(ctxTxn, txn) {
		t.Errorf("expected txn from context (%#v) to equal returned txn (%#v)", ctxTxn, txn)
	}
}
