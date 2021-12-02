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

type DBs []DB

type DB struct {
	db  *sql.DB
	cfg config.Database
}

func NewDB(db *sql.DB, cfg config.Database) DB {
	return DB{
		db:  db,
		cfg: cfg,
	}
}

func (dbs DBs) Each(callback func(db DB)) {
	for _, db := range dbs {
		callback(db)
	}
}

func (db DB) Config() config.Database {
	return db.cfg
}

func (db DB) Connection() *sql.DB {
	return db.db
}

func init() {
	setupLoggers()
}

func setupLoggers() {
	err := mysql.SetLogger(log.Logger)
	if err != nil {
		log.Logger.WithError(err).Fatalf("unable to set up JSON logger for MySQL driver")
	}
}

// NewDBs creates a database connection for each configured database in config.
// It will also apply migrations on the databases automatically, unless migrations
// are disabled in config.
func NewDBs(cfg *config.Config) (DBs, func()) {
	dbs := make(DBs, len(cfg.DBs))

	log.Logger.Debug("connecting to the database")

	for i, dbCfg := range cfg.DBs {
		db, err := sql.Open(dbCfg.Driver.String(), dbCfg.GetDSN())
		if err != nil {
			log.Logger.Fatalf("unable to connect to the database: %s", err)
		}

		db.SetMaxOpenConns(maxOpenConnections)
		db.SetMaxIdleConns(maxIdleConnections)
		db.SetConnMaxLifetime(maxConnectionLifetime)

		connectToDatabase(dbCfg, db, cfg.SkipMigrations)
		dbs[i] = NewDB(db, dbCfg)
	}

	cleanup := func() {
		for _, db := range dbs {
			if err := db.db.Close(); err != nil {
				log.Logger.WithError(err).Error("error closing database during shutdown process")
			}
		}
	}

	return dbs, cleanup
}

func connectToDatabase(dbCfg config.Database, db *sql.DB, skipMigrations bool) {
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

	if skipMigrations {
		log.Logger.Info("skipping database migrations because they are disabled")
		return
	}

	migrateDatabase(dbCfg.Driver, dbCfg.Name, db)
}
