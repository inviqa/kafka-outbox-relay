package newrelic

import (
	"context"

	"github.com/newrelic/go-agent/v3/newrelic"
)

// ContextWithTxn will start a new transaction with the given name, using the provided
// *newrelic.Application value. If this value is nil, then an empty *newrelic.Transaction value
// will be created.
func ContextWithTxn(parent context.Context, name string, app *newrelic.Application) (context.Context, *newrelic.Transaction) {
	var txn *newrelic.Transaction
	if app == nil {
		txn = &newrelic.Transaction{}
	} else {
		txn = app.StartTransaction(name)
	}

	return newrelic.NewContext(parent, txn), txn
}
