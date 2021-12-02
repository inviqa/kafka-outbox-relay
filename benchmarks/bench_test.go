//go:build benchmarks
// +build benchmarks

package benchmarks

import (
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
	db           data.DB
	pub          kafka.Publisher
	syncProducer *benchkafka.SyncProducer
)

func init() {
	cfg = createConfig()

	dbs, _ := data.NewDBs(cfg)
	db = dbs[0]

	ensureOutboxTableExists()

	repo = outbox.NewRepository(dbs[0], cfg)
	syncProducer = benchkafka.NewSyncProducer(cfg.KafkaHost)
	pub = kafka.NewPublisherWithProducer(syncProducer)
}

func purgeOutboxTable() {
	_, err := db.Connection().Exec(fmt.Sprintf("TRUNCATE TABLE `%s`;", db.Config().OutboxTable))
	if err != nil {
		panic(fmt.Sprintf("an error occurred cleaning the outbox table for tests: %s", err))
	}
}

func ensureOutboxTableExists() {
	_, err := db.Connection().Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s LIKE kafka_outbox;", db.Config().OutboxTable))
	if err != nil {
		panic(fmt.Sprintf("an error occurred creating the outbox table for integration tests: %s", err))
	}
}

func insertOutboxMessages(msgs []*outbox.Message) {
	tx, err := db.Connection().Begin()
	if err != nil {
		panic(fmt.Sprintf("error creating a DB transaction: %s", err))
	}

	for _, msg := range msgs {
		q := fmt.Sprintf("INSERT INTO `%s` SET topic = ?, payload_json = ?, payload_headers = '{}';", db.Config().OutboxTable)

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
		DBs: []config.Database{
			{
				Host:        "localhost",
				Port:        13306,
				User:        "kafka-outbox-relay",
				Password:    "kafka-outbox-relay",
				Name:        "kafka-outbox-relay",
				Driver:      config.MySQL,
				OutboxTable: "kafka_outbox_test",
			},
		},
		KafkaHost:       []string{"localhost:9092"},
		PollFrequencyMs: 500,
	}

	return cfg
}
