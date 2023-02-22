package job

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	nr "github.com/newrelic/go-agent/v3/newrelic"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/newrelic"
	"inviqa/kafka-outbox-relay/outbox/data"
)

type Optimizer interface {
	Execute(ctx context.Context) error
	EnableSideCarProxyQuit(proxyUrl string)
}

func RunOptimize(parent context.Context, nrApp *nr.Application, dbs data.DBs, cfg *config.Config) int {
	ctx, txn := newrelic.ContextWithTxn(parent, "run optimize", nrApp)
	defer txn.End()

	var exitCode int
	dbs.Each(func(db data.DB) {
		exitCode += runOptimizeOnDb(ctx, db, cfg.SidecarProxyUrl)
	})
	return normalizeExitCode(exitCode)
}

func runOptimizeOnDb(ctx context.Context, db data.DB, sidecarProxyUrl string) int {
	txn := nr.FromContext(ctx)
	defer txn.StartSegment("runOptimizeOnDb() " + db.Config().Driver.String()).End()

	dbCfg := db.Config()
	j := newOptimizeTableWithDefaultClient(db.Connection(), dbCfg.OutboxTable, dbCfg.Driver)
	if j == nil {
		txn.NoticeError(fmt.Errorf("unable to determine the database driver: %s", dbCfg.Driver))
		log.Logger.WithField("config", dbCfg).Fatalf("unable to determine the database driver")
		return 1
	}

	if sidecarProxyUrl != "" {
		j.EnableSideCarProxyQuit(sidecarProxyUrl)
	}

	err := j.Execute(ctx)
	if err != nil {
		txn.NoticeError(err)
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
