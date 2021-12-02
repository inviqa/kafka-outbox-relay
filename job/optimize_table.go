package job

import (
	"database/sql"
	"net/http"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox/data"
)

type Optimizer interface {
	Execute() error
	EnableSideCarProxyQuit(proxyUrl string)
}

func RunOptimize(dbs data.DBs, cfg *config.Config) int {
	var exitCode int
	dbs.Each(func(db data.DB) {
		exitCode += runOptimizeOnDb(db, cfg.SidecarProxyUrl)
	})
	return normalizeExitCode(exitCode)
}

func runOptimizeOnDb(db data.DB, sidecarProxyUrl string) int {
	dbCfg := db.Config()
	j := newOptimizeTableWithDefaultClient(db.Connection(), dbCfg.OutboxTable, dbCfg.Driver)
	if j == nil {
		log.Logger.WithField("config", dbCfg).Fatalf("unable to determine the database driver")
		return 1
	}

	if sidecarProxyUrl != "" {
		j.EnableSideCarProxyQuit(sidecarProxyUrl)
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
