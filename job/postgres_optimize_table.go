package job

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/newrelic/go-agent/v3/newrelic"

	"inviqa/kafka-outbox-relay/log"
)

type postgresOptimizeTable struct {
	Db        *sql.DB
	TableName string
	SidecarQuitter
}

func (o *postgresOptimizeTable) Execute(ctx context.Context) error {
	defer o.newRelicSegment(ctx, "VACUUM").End()

	_, err := o.Db.Exec(fmt.Sprintf("VACUUM %s;", o.TableName))

	if err == nil {
		log.Logger.Info("optimized Postgres outbox table successfully")
	} else {
		log.Logger.WithError(err).Error("an error occurred optimizing the Postgres outbox table")
	}

	if o.QuitSidecar {
		err = o.Quit()
		if err != nil {
			return err
		}
	}

	return err
}

func (o *postgresOptimizeTable) newRelicSegment(ctx context.Context, operation string) *newrelic.DatastoreSegment {
	return &newrelic.DatastoreSegment{
		Product:    newrelic.DatastorePostgres,
		Collection: o.TableName,
		Operation:  operation,
		StartTime:  newrelic.FromContext(ctx).StartSegmentNow(),
	}
}
