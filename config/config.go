package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"inviqa/kafka-outbox-relay/log"

	"github.com/alexflint/go-arg"
)

const (
	MySQL    DbDriver = "mysql"
	Postgres DbDriver = "postgres"

	defaultPublishAttempts = 3
	outboxTable            = "kafka_outbox"
)

type DbDriver string

var supportedDbTypes = map[DbDriver]bool{
	Postgres: true,
	MySQL:    true,
}

type args struct {
	PollingDisabled      bool     `arg:"--polling-disabled,env:POLLING_DISABLED"`
	SkipMigrations       bool     `arg:"--skip-migrations,env:SKIP_MIGRATIONS"`
	DBHost               string   `arg:"--db-host,env:DB_HOST,required"`
	DBPort               uint32   `arg:"--db-port,env:DB_PORT,required"`
	DBUser               string   `arg:"--db-user,env:DB_USER,required"`
	DBPass               string   `arg:"--db-pass,env:DB_PASS,required"`
	DBNames              []string `arg:"--db-name,env:DB_NAME,required"`
	DBDriver             DbDriver `arg:"--db-driver,env:DB_DRIVER,required"`
	DBOutboxTable        string
	KafkaHost            []string `arg:"--kafka-host,env:KAFKA_HOST"`
	KafkaPublishAttempts int      `arg:"--kafka-publish-attempts,env:KAFKA_PUBLISH_ATTEMPTS"`
	TLSEnable            bool     `arg:"--kafka-tls,env:TLS_ENABLE"`
	TLSSkipVerifyPeer    bool     `arg:"--kafka-tls-verify-peer,env:TLS_SKIP_VERIFY_PEER"`
	WriteConcurrency     int      `arg:"--write-concurrency,env:WRITE_CONCURRENCY"`
	PollFrequencyMs      int      `arg:"--poll-frequency-ms,env:POLL_FREQUENCY_MS"`
	RunCleanup           bool     `arg:"--cleanup,env:RUN_CLEANUP"`
	RunOptimize          bool     `arg:"--optimize,env:RUN_OPTIMIZE"`
	SidecarProxyUrl      string   `arg:"--sidecar-proxy-url,env:SIDECAR_PROXY_URL"`
	BatchSize            int      `arg:"--batch-size,env:BATCH_SIZE"`
}

type Database struct {
	Host, User, Password, Name, OutboxTable string
	Port                                    uint32
	Driver                                  DbDriver
	TLSSkipVerifyPeer                       bool
	TLSEnable                               bool
}

type Config struct {
	PollingDisabled      bool
	SkipMigrations       bool
	DBs                  []Database
	KafkaHost            []string
	KafkaPublishAttempts int
	TLSEnable            bool
	TLSSkipVerifyPeer    bool
	WriteConcurrency     int
	PollFrequencyMs      int
	RunCleanup           bool
	RunOptimize          bool
	SidecarProxyUrl      string
	BatchSize            int
}

func NewConfig() (*Config, error) {
	a := &args{
		KafkaPublishAttempts: defaultPublishAttempts,
		DBOutboxTable:        outboxTable,
		WriteConcurrency:     1,
		PollFrequencyMs:      500,
		BatchSize:            250,
	}
	arg.MustParse(a)

	if !supportedDbTypes[a.DBDriver] {
		return nil, fmt.Errorf("the DB_DRIVER provided (%s) is not supported", a.DBDriver)
	}

	return &Config{
		PollingDisabled:      a.PollingDisabled,
		SkipMigrations:       a.SkipMigrations,
		KafkaHost:            a.KafkaHost,
		DBs:                  databasesConfig(a),
		KafkaPublishAttempts: a.KafkaPublishAttempts,
		TLSEnable:            a.TLSEnable,
		TLSSkipVerifyPeer:    a.TLSSkipVerifyPeer,
		WriteConcurrency:     a.WriteConcurrency,
		PollFrequencyMs:      a.PollFrequencyMs,
		RunCleanup:           a.RunCleanup,
		RunOptimize:          a.RunOptimize,
		SidecarProxyUrl:      a.SidecarProxyUrl,
		BatchSize:            a.BatchSize,
	}, nil
}

// databasesConfig models database configuration, and currently creates a separate database config
// for each database name that is provided from env
// NOTE: In the future, we will likely expand this to allow multiple database connection details to
// be provided via env vars too
func databasesConfig(a *args) []Database {
	var dbs []Database
	for _, dbName := range a.DBNames {
		dbs = append(dbs, Database{
			Host:              a.DBHost,
			Port:              a.DBPort,
			User:              a.DBUser,
			Password:          a.DBPass,
			Name:              dbName,
			Driver:            a.DBDriver,
			OutboxTable:       a.DBOutboxTable,
			TLSEnable:         a.TLSEnable,
			TLSSkipVerifyPeer: a.TLSSkipVerifyPeer,
		})
	}
	return dbs
}

func (c *Config) GetPollIntervalDurationInMs() time.Duration {
	return time.Duration(c.PollFrequencyMs) * time.Millisecond
}

func (d Database) GetDSN() string {
	switch d.Driver {
	case MySQL:
		tls := "false"
		if d.TLSEnable {
			if d.TLSSkipVerifyPeer {
				tls = "skip-verify"
			} else {
				tls = "true"
			}
		}
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&tls=%s&multiStatements=true", d.User, d.Password, d.Host, d.Port, d.Name, tls)
	case Postgres:
		sslMode := "disable"
		if d.TLSEnable {
			if d.TLSSkipVerifyPeer {
				sslMode = "require"
			} else {
				sslMode = "verify-full"
			}
		}
		return fmt.Sprintf("%s://%s@%s:%d/%s?sslmode=%s", d.Driver, url.UserPassword(d.User, d.Password), d.Host, d.Port, d.Name, sslMode)
	default:
		log.Logger.Fatalf("the DB driver configured (%s) is not supported", d.Driver)
		return ""
	}
}

func (c *Config) GetDependencySystemAddresses() []string {
	return c.KafkaHost
}

func (c Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"PollingDisabled":      c.PollingDisabled,
		"SkipMigrations":       c.SkipMigrations,
		"Databases":            c.DBs,
		"KafkaHost":            c.KafkaHost,
		"KafkaPublishAttempts": c.KafkaPublishAttempts,
		"TLSEnable":            c.TLSEnable,
		"TLSSkipVerifyPeer":    c.TLSSkipVerifyPeer,
		"WriteConcurrency":     c.WriteConcurrency,
		"PollFrequencyMs":      c.PollFrequencyMs,
		"RunCleanup":           c.RunCleanup,
		"RunOptimize":          c.RunOptimize,
		"SidecarProxyUrl":      c.SidecarProxyUrl,
		"BatchSize":            c.BatchSize,
	})
}

func (d Database) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"Port":              d.Port,
		"User":              d.User,
		"Pass":              "xxxxx",
		"Name":              d.Name,
		"Driver":            d.Driver,
		"OutboxTable":       d.OutboxTable,
		"TLSEnable":         d.TLSEnable,
		"TLSSkipVerifyPeer": d.TLSSkipVerifyPeer,
	})
}

func (d DbDriver) MySQL() bool {
	return d == MySQL
}

func (d DbDriver) Postgres() bool {
	return d == Postgres
}

func (d DbDriver) String() string {
	return string(d)
}
