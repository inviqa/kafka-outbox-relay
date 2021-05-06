package job

import (
	"database/sql"
	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/log"
	"net/http"

	"github.com/sirupsen/logrus"
)

type Optimizer interface {
	Execute() error
	EnableSideCarProxyQuit(proxyUrl string)
}

func RunOptimize(db *sql.DB, cfg *config.Config) int {
	j := newOptimizeTableWithDefaultClient(db, cfg.DBOutboxTable, cfg.DBDriver)
	if j == nil {
		log.Logger.WithFields(logrus.Fields{
			"config": cfg,
		}).Fatalf("unable to determine the database driver")
		return 1
	}

	if cfg.SidecarProxyUrl != "" {
		j.EnableSideCarProxyQuit(cfg.SidecarProxyUrl)
	}

	err := j.Execute()
	if err != nil {
		return 1
	}

	return 0
}

func newOptimizeTableWithDefaultClient(db *sql.DB, tableName string, dr config.DbDriver) Optimizer {
	return newOptimizeTable(db, tableName, dr, http.DefaultClient)
}

func newOptimizeTable(db *sql.DB, tableName string, dr config.DbDriver, cl httpPoster) Optimizer {
	sc := SidecarQuitter{Client: cl}
	switch true {
	case dr.MySQL():
		return &mysqlOptimizeTable{
			Db:             db,
			TableName:      tableName,
			SidecarQuitter: sc,
		}
	case dr.Postgres():
		return &postgresOptimizeTable{
			Db:             db,
			TableName:      tableName,
			SidecarQuitter: sc,
		}
	}
	return nil
}
