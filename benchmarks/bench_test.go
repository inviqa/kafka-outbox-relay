//go:build benchmarks
// +build benchmarks

package benchmarks

import (
	"database/sql"
	"fmt"

	benchkafka "inviqa/kafka-outbox-relay/benchmarks/kafka"
	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/kafka"
	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/data"
)

var (
	repo         outbox.Repository
	cfg          *config.Config
	db           *sql.DB
	pub          kafka.Publisher
	syncProducer *benchkafka.SyncProducer
)

func init() {
	cfg = createConfig()

	db = data.NewDBs(cfg)
	ensureOutboxTableExists()

	repo = outbox.NewRepository(db, cfg)
	syncProducer = benchkafka.NewSyncProducer(cfg.KafkaHost)
	pub = kafka.NewPublisherWithProducer(syncProducer)
}

func purgeOutboxTable() {
	_, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`;", cfg.DBOutboxTable))
	if err != nil {
		panic(fmt.Sprintf("an error occurred cleaning the outbox table for tests: %s", err))
	}
}

func ensureOutboxTableExists() {
	_, err := db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s LIKE kafka_outbox;", cfg.DBOutboxTable))
	if err != nil {
		panic(fmt.Sprintf("an error occurred creating the outbox table for integration tests: %s", err))
	}
}

func insertOutboxMessages(msgs []*outbox.Message) {
	tx, err := db.Begin()
	if err != nil {
		panic(fmt.Sprintf("error creating a DB transaction: %s", err))
	}

	for _, msg := range msgs {
		q := fmt.Sprintf("INSERT INTO `%s` SET topic = ?, payload_json = ?, payload_headers = '{}';", cfg.DBOutboxTable)

		_, err = tx.Exec(q, msg.Topic, msg.PayloadJson)
		if err != nil {
			panic(fmt.Sprintf("failed to insert outbox message in DB: %s", err))
		}
	}

	err = tx.Commit()
	if err != nil {
		panic(fmt.Sprintf("error committing DB transaction: %s", err))
	}
}

func createConfig() *config.Config {
	cfg = &config.Config{
		DBHost:          "localhost",
		DBPort:          13306,
		DBUser:          "kafka-outbox-relay",
		DBPass:          "kafka-outbox-relay",
		DBName:          "kafka-outbox-relay",
		DBDriver:        config.MySQL,
		DBOutboxTable:   "kafka_outbox_test",
		KafkaHost:       []string{"localhost:9092"},
		PollFrequencyMs: 500,
	}

	return cfg
}
