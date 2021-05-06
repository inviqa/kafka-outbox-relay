package job

import (
	"database/sql"
	"fmt"

	"inviqa/kafka-outbox-relay/log"
)

type mysqlOptimizeTable struct {
	Db        *sql.DB
	TableName string
	SidecarQuitter
}

func (o *mysqlOptimizeTable) Execute() error {
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
