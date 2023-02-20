package job

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/newrelic/go-agent/v3/newrelic"

	"inviqa/kafka-outbox-relay/log"
)

type mysqlOptimizeTable struct {
	Db        *sql.DB
	TableName string
	SidecarQuitter
}

func (o *mysqlOptimizeTable) Execute(ctx context.Context) error {
	defer o.newRelicSegment(ctx, "OPTIMIZE TABLE").End()

	_, err := o.Db.Exec(fmt.Sprintf("OPTIMIZE TABLE %s;", o.TableName))

	if err == nil {
		log.Logger.Info("optimized MySQL outbox table successfully")
	} else {
		log.Logger.WithError(err).Error("an error occurred optimizing the MySQL outbox table")
	}

	if o.QuitSidecar {
		err = o.Quit()
		if err != nil {
			return err
		}
	}

	return err
}

func (o *mysqlOptimizeTable) newRelicSegment(ctx context.Context, operation string) *newrelic.DatastoreSegment {
	return &newrelic.DatastoreSegment{
		Product:    newrelic.DatastoreMySQL,
		Collection: o.TableName,
		Operation:  operation,
		StartTime:  newrelic.FromContext(ctx).StartSegmentNow(),
	}
}
