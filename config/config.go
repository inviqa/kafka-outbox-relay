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
)

type DbDriver string

var supportedDbTypes = map[DbDriver]bool{
	Postgres: true,
	MySQL:    true,
}

type Config struct {
	SkipMigrations       bool     `arg:"--skip-migrations,env:SKIP_MIGRATIONS"`
	DBHost               string   `arg:"--db-host,env:DB_HOST,required"`
	DBPort               uint32   `arg:"--db-port,env:DB_PORT,required"`
	DBUser               string   `arg:"--db-user,env:DB_USER,required"`
	DBPass               string   `arg:"--db-pass,env:DB_PASS,required"`
	DBSchema             string   `arg:"--db-schema,env:DB_SCHEMA,required"`
	DBDriver             DbDriver `arg:"--db-driver,env:DB_DRIVER,required"`
	DBOutboxTable        string   `arg:"--db-outbox-table,env:DB_OUTBOX_TABLE,required"`
	KafkaHost            []string `arg:"--kafka-host,env:KAFKA_HOST,required"`
	KafkaPublishAttempts int      `arg:"--kafka-publish-attempts,env:KAFKA_PUBLISH_ATTEMPTS,required"`
	TLSEnable            bool     `arg:"--kafka-tls,env:TLS_ENABLE"`
	TLSSkipVerifyPeer    bool     `arg:"--kafka-tls-verify-peer,env:TLS_SKIP_VERIFY_PEER"`
	WriteConcurrency     int      `arg:"--write-concurrency,env:WRITE_CONCURRENCY"`
	PollFrequencyMs      int      `arg:"--poll-frequency-ms,env:POLL_FREQUENCY_MS"`
	RunCleanup           bool     `arg:"--cleanup,env:RUN_CLEANUP"`
	RunOptimize          bool     `arg:"--optimize,env:RUN_OPTIMIZE"`
	SidecarProxyUrl      string   `arg:"--sidecar-proxy-url,env:SIDECAR_PROXY_URL"`
	BatchSize            int      `arg:"--batch-size,env:BATCH_SIZE"`
}

func NewConfig() (*Config, error) {
	c := &Config{
		WriteConcurrency: 1,
		PollFrequencyMs:  500,
		BatchSize:        250,
	}
	arg.MustParse(c)

	if !supportedDbTypes[c.DBDriver] {
		return nil, fmt.Errorf("the DB_DRIVER provided (%s) is not supported", c.DBDriver)
	}

	return c, nil
}

func (c *Config) GetPollIntervalDurationInMs() time.Duration {
	return time.Duration(c.PollFrequencyMs) * time.Millisecond
}

func (c *Config) GetDSN() string {
	switch c.DBDriver {
	case MySQL:
		tls := "false"
		if c.TLSEnable {
			if c.TLSSkipVerifyPeer {
				tls = "skip-verify"
			} else {
				tls = "true"
			}
		}
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&tls=%s&multiStatements=true", c.DBUser, c.DBPass, c.DBHost, c.DBPort, c.DBSchema, tls)
	case Postgres:
		sslMode := "disable"
		if c.TLSEnable {
			if c.TLSSkipVerifyPeer {
				sslMode = "require"
			} else {
				sslMode = "verify-full"
			}
		}
		return fmt.Sprintf("%s://%s@%s:%d/%s?sslmode=%s", c.DBDriver, url.UserPassword(c.DBUser, c.DBPass), c.DBHost, c.DBPort, c.DBSchema, sslMode)
	default:
		log.Logger.Fatalf("the DB driver configured (%s) is not supported", c.DBDriver)
		return ""
	}
}

func (c *Config) GetDependencySystemAddresses() []string {
	return c.KafkaHost
}

func (c Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"SkipMigrations":       c.SkipMigrations,
		"DBHost":               c.DBHost,
		"DBPort":               c.DBPort,
		"DBUser":               c.DBUser,
		"DBPass":               "xxxxx",
		"DBSchema":             c.DBSchema,
		"DBDriver":             c.DBDriver,
		"DBOutboxTable":        c.DBOutboxTable,
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

func (d DbDriver) MySQL() bool {
	return d == MySQL
}

func (d DbDriver) Postgres() bool {
	return d == Postgres
}

func (d DbDriver) String() string {
	return string(d)
}
