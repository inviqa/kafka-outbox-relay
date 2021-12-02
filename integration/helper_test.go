//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"time"

	"inviqa/kafka-outbox-relay/config"
	h "inviqa/kafka-outbox-relay/integration/http"
	testkafka "inviqa/kafka-outbox-relay/integration/kafka"
	"inviqa/kafka-outbox-relay/kafka"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/data"
	"inviqa/kafka-outbox-relay/outbox/poller"
	"inviqa/kafka-outbox-relay/outbox/processor"

	"github.com/Shopify/sarama"
)

const (
	testModeDocker = "docker"
)

var (
	cfg              *config.Config
	db               *sql.DB
	syncProducer     *testkafka.SyncProducer
	repo             outbox.Repository
	publishedDeleter outbox.Repository
	server           *httptest.Server
	pub              kafka.Publisher
)

func init() {
	server = httptest.NewServer(h.GetHttpTestHandlerFunc())
	setupConfig()

	syncProducer = testkafka.NewSyncProducer(cfg.KafkaHost)
	pub = kafka.NewPublisherWithProducer(syncProducer)

	dbs, _ := data.NewDBs(cfg)
	db = dbs[0]
	ensureOutboxTableExists()
	purgeOutboxTable()

	repo = outbox.NewRepository(db, cfg)
	publishedDeleter = repo

	go pollForMessages()
}

func returnErrorFromSyncProducerForMessage(msgBody string, err error) {
	syncProducer.AddError(msgBody, err)
}

//gocyclo:ignore
func consumeFromKafkaUntilMessagesReceived(exp []testkafka.MessageExpectation) *testkafka.ConsumerHandler {
	doneCh := make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())

	toFind := make([]testkafka.MessageExpectation, len(exp))
	copy(toFind, exp)
	cons := &testkafka.ConsumerHandler{
		Consume: func(consumed *sarama.ConsumerMessage, c *testkafka.ConsumerHandler) {
			j := 0
			for _, m := range toFind {
				headersAreSame := reflect.DeepEqual(consumed.Headers, m.Headers)
				keysAreSame := bytes.Equal(consumed.Key, m.Key)
				if !headersAreSame || !keysAreSame || bytes.Compare(m.Msg.PayloadJson, consumed.Value) != 0 {
					toFind[j] = m
					j++
				}
			}
			toFind = toFind[:j]
			if len(toFind) == 0 {
				c.MessagesFound = true
			}
		},
	}

	cl, err := sarama.NewConsumerGroup(cfg.KafkaHost, "test-cons", kafka.NewSaramaConfig(false, false))
	if err != nil {
		log.Logger.WithError(err).Panic("error occurred creating Kafka consumer group client")
	}

	topics := testkafka.GetTopicsFromMessageExpectations(exp)
	go func() {
		for {
			log.Logger.Debugf("about to consume topics %s", topics)
			if err := cl.Consume(ctx, topics, cons); err != nil {
				log.Logger.WithError(err).Panic("error when consuming from Kafka")
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	go func() {
		for {
			if cons.MessagesFound {
				doneCh <- true
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	select {
	case <-time.After(10 * time.Second):
		break
	case <-doneCh:
		break
	}

	cancel()

	if err := cl.Close(); err != nil {
		log.Logger.WithError(err).Panic("error occurred closing Kafka client")
	}

	return cons
}

func setupConfig() *config.Config {
	var runInDocker bool
	if os.Getenv("GO_TEST_MODE") == testModeDocker {
		runInDocker = true
	}

	cfg = &config.Config{
		PollFrequencyMs:      1000,
		SidecarProxyUrl:      server.URL,
		KafkaPublishAttempts: 3,
		BatchSize:            250,
		KafkaHost:            []string{"localhost:9092"},
	}

	envs := map[string]string{}
	for _, env := range os.Environ() {
		pts := strings.Split(env, "=")
		envs[pts[0]] = pts[1]
	}

	dbCfg := config.Database{
		User:        "kafka-outbox-relay",
		Password:    "kafka-outbox-relay",
		Name:        "kafka-outbox-relay",
		OutboxTable: "kafka_outbox_test",
	}
	if envs["DB_DRIVER"] == string(config.MySQL) {
		dbCfg.Driver = config.MySQL
		dbCfg.Port = 13306
	} else {
		dbCfg.Driver = config.Postgres
		dbCfg.Port = 15432
	}

	if runInDocker {
		dbCfg.Host = dbCfg.Driver.String()
		dbCfg.Port = dbCfg.Port - 10000
		cfg.KafkaHost = []string{"kafka:29092"}
	} else {
		dbCfg.Host = "localhost"
	}

	cfg.DBs = []config.Database{dbCfg}

	return cfg
}

func pollForMessages() {
	batchCh := make(chan *outbox.Batch, 10)

	go poller.New(repo, batchCh).Poll(context.Background(), time.Millisecond*100)

	processor.NewBatchProcessor(repo, pub).ListenAndProcess(context.Background(), batchCh)
}

func waitForBatchToBePolled() {
	time.Sleep(time.Millisecond * 100)
}
