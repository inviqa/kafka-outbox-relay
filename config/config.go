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

	dummyMode Mode = "dummy"
	realMode  Mode = "real"

	outboxTable = "kafka_outbox"
)

type Mode string
type DbDriver string

var supportedDbTypes = map[DbDriver]bool{
	Postgres: true,
	MySQL:    true,
}

type Config struct {
	Mode                 Mode     `arg:"--mode,env:MODE"`
	SkipMigrations       bool     `arg:"--skip-migrations,env:SKIP_MIGRATIONS"`
	DBHost               string   `arg:"--db-host,env:DB_HOST,required"`
	DBPort               uint32   `arg:"--db-port,env:DB_PORT,required"`
	DBUser               string   `arg:"--db-user,env:DB_USER,required"`
	DBPass               string   `arg:"--db-pass,env:DB_PASS,required"`
	DBName               string   `arg:"--db-name,env:DB_NAME,required"`
	DBDriver             DbDriver `arg:"--db-driver,env:DB_DRIVER,required"`
	DBOutboxTable        string
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
		DBOutboxTable:    outboxTable,
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

func (c *Config) InDummyMode() bool {
	return c.Mode == dummyMode
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
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&tls=%s&multiStatements=true", c.DBUser, c.DBPass, c.DBHost, c.DBPort, c.DBName, tls)
	case Postgres:
		sslMode := "disable"
		if c.TLSEnable {
			if c.TLSSkipVerifyPeer {
				sslMode = "require"
			} else {
				sslMode = "verify-full"
			}
		}
		return fmt.Sprintf("%s://%s@%s:%d/%s?sslmode=%s", c.DBDriver, url.UserPassword(c.DBUser, c.DBPass), c.DBHost, c.DBPort, c.DBName, sslMode)
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
		"Mode":                 c.Mode,
		"SkipMigrations":       c.SkipMigrations,
		"DBHost":               c.DBHost,
		"DBPort":               c.DBPort,
		"DBUser":               c.DBUser,
		"DBPass":               "xxxxx",
		"DBName":               c.DBName,
		"DBDriver":             c.DBDriver,
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

func (m Mode) String() string {
	return string(m)
}
