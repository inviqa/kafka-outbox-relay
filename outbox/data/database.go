package data

import (
	"database/sql"
	"time"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/log"

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	connectionAttempts    = 30
	maxOpenConnections    = 10
	maxIdleConnections    = 5
	maxConnectionLifetime = time.Minute * 1
)

func init() {
	setupLoggers()
}

func setupLoggers() {
	err := mysql.SetLogger(log.Logger)
	if err != nil {
		log.Logger.WithError(err).Fatalf("unable to set up JSON logger for MySQL driver")
	}
}

func NewDB(cfg *config.Config) *sql.DB {
	log.Logger.Debug("connecting to the database")

	db, err := sql.Open(cfg.DBDriver.String(), cfg.GetDSN())
	if err != nil {
		log.Logger.Fatalf("unable to connect to the database: %s", err)
	}

	db.SetMaxOpenConns(maxOpenConnections)
	db.SetMaxIdleConns(maxIdleConnections)
	db.SetConnMaxLifetime(maxConnectionLifetime)

	tries := connectionAttempts
	for {
		err := db.Ping()
		if err == nil {
			break
		}

		time.Sleep(time.Second * 1)
		tries--
		log.Logger.Infof("database is not available (err: %s), retrying %d more time(s)", err, tries)

		if tries == 0 {
			log.Logger.Fatalf("database did not become available within %d connection attempts", connectionAttempts)
		}
	}

	return db
}
