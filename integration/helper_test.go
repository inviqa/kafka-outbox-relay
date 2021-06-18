// +build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"inviqa/kafka-outbox-relay/config"
	h "inviqa/kafka-outbox-relay/integration/http"
	testkafka "inviqa/kafka-outbox-relay/integration/kafka"
	"inviqa/kafka-outbox-relay/kafka"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/data"
	"inviqa/kafka-outbox-relay/outbox/poller"
	"inviqa/kafka-outbox-relay/outbox/processor"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"time"

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
	cfg = getConfig()

	syncProducer = testkafka.NewSyncProducer(cfg.KafkaHost)
	pub = kafka.NewPublisherWithProducer(syncProducer)

	db = data.NewDB(cfg)
	data.MigrateDatabase(db, cfg)
	ensureOutboxTableExists()
	purgeOutboxTable()

	repo = outbox.NewRepository(db, cfg)
	publishedDeleter = repo
	batchCh := make(chan *outbox.Batch, 10)

	p := poller.New(repo, batchCh, context.Background())
	go p.Poll(cfg.GetPollIntervalDurationInMs())

	proc := processor.NewBatchProcessor(repo, pub, context.Background())
	go proc.ListenAndProcess(batchCh)
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
		MessagesFound: false,
		Consume: func(consumed *sarama.ConsumerMessage, c *testkafka.ConsumerHandler) {
			j := 0
			for _, m := range toFind {
				headersAreSame := reflect.DeepEqual(consumed.Headers, m.Headers)
				if !headersAreSame || bytes.Compare(m.Msg.PayloadJson, consumed.Value) != 0 {
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

	go func() {
		for err := range cl.Errors() {
			log.Logger.WithError(err).Errorf("error occurred in consumer group")
		}
	}()

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

func getConfig() *config.Config {
	var runInDocker bool
	if os.Getenv("GO_TEST_MODE") == testModeDocker {
		runInDocker = true
	}

	cfg := &config.Config{
		EnableMigrations: true,
		DBOutboxTable:    "kafka_outbox_test",
		PollFrequencyMs:  500,
		SidecarProxyUrl:  server.URL,
		BatchSize:        250,
		KafkaHost:        []string{"localhost:9092"},
		DBUser:           "kafka-outbox-relay",
		DBPass:           "kafka-outbox-relay",
		DBSchema:         "kafka-outbox-relay",
	}

	envs := map[string]string{}
	for _, env := range os.Environ() {
		pts := strings.Split(env, "=")
		envs[pts[0]] = pts[1]
	}

	if envs["DB_DRIVER"] == string(config.MySQL) {
		cfg.DBDriver = config.MySQL
		cfg.DBPort = 13306
	} else {
		cfg.DBDriver = config.Postgres
		cfg.DBPort = 15432
	}

	if runInDocker {
		cfg.DBHost = cfg.DBDriver.String()
		cfg.DBPort = cfg.DBPort - 10000
		cfg.KafkaHost = []string{"kafka:29092"}
	} else {
		cfg.DBHost = "localhost"
	}

	return cfg
}
