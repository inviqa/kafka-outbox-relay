package job

import (
	"database/sql"
	"fmt"

	"inviqa/kafka-outbox-relay/log"
)

type postgresOptimizeTable struct {
	Db        *sql.DB
	TableName string
	SidecarQuitter
}

func (o *postgresOptimizeTable) Execute() error {
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
