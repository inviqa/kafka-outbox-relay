package data

import (
	"database/sql"
	"embed"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/log"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

const (
	migrationsTable = "kafka_outbox_schema_migrations"
)

var (
	//go:embed migrations/mysql/*.sql
	mysqlFiles embed.FS
	//go:embed migrations/postgres/*.sql
	postgresFiles embed.FS
)

func migrateDatabase(cfgDriver config.DbDriver, databaseName string, db *sql.DB) {
	log.Logger.Infof("checking database migrations for '%s'", databaseName)

	var err error
	var driver database.Driver
	if cfgDriver.MySQL() {
		driver, err = mysql.WithInstance(db, &mysql.Config{MigrationsTable: migrationsTable})
	} else {
		driver, err = postgres.WithInstance(db, &postgres.Config{MigrationsTable: migrationsTable})
	}

	if err != nil {
		log.Logger.Fatalf("unable to create migration instance from database: %s", err)
	}

	d := createMigrateSourceDriver(cfgDriver)
	m, err := migrate.NewWithInstance("iofs", d, databaseName, driver)
	if err != nil {
		log.Logger.Fatalf("failed to load migration files from source driver: %s", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Logger.Fatalf("failed to migrate database: %s", err)
	}
}

func createMigrateSourceDriver(driver config.DbDriver) source.Driver {
	var d source.Driver
	var err error

	switch driver {
	case config.MySQL:
		d, err = iofs.New(mysqlFiles, "migrations/mysql")
	case config.Postgres:
		d, err = iofs.New(postgresFiles, "migrations/postgres")
	}

	if err != nil {
		log.Logger.Fatalf("unable to load migration files from embedded filesystem: %s", err)
	}

	return d
}
