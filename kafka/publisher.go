package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"

	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox"

	"github.com/Shopify/sarama"
)

type Publisher struct {
	producer sarama.SyncProducer
}

func NewPublisher(kafkaHost []string, cfg *sarama.Config) Publisher {
	return NewPublisherWithProducer(newProducer(cfg, kafkaHost))
}

func NewPublisherWithProducer(prod sarama.SyncProducer) Publisher {
	return Publisher{
		producer: prod,
	}
}

func newProducer(cfg *sarama.Config, kafkaHosts []string) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer(kafkaHosts, cfg)
	if err != nil {
		log.Logger.Panicf("could not start kafka producer: %s", err)
	}

	return producer
}

func (p Publisher) PublishMessage(m *outbox.Message) error {
	headers, err := p.createRecordHeaders(m.PayloadHeaders)
	if err != nil {
		wrapErr := fmt.Errorf("error unmarshalling message headers for publishing to Kafka: %w", err)
		log.Logger.Error(wrapErr)
		return wrapErr
	}

	partition, offset, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic:   m.Topic,
		Headers: headers,
		Value:   sarama.ByteEncoder(m.PayloadJson),
		Key:     newMessageKey(m.Key, m.PartitionKey),
	})

	if err != nil {
		wrapErr := fmt.Errorf("error producing message in Kafka: %w", err)
		log.Logger.Error(wrapErr)
		return wrapErr
	}

	log.Logger.Debugf("produced message in Kafka (topic: %s, partition: %d, offset: %d)", m.Topic, partition, offset)

	return nil
}

func (p Publisher) Close() error {
	return p.producer.Close()
}

func (p Publisher) createRecordHeaders(headers []byte) ([]sarama.RecordHeader, error) {
	emptyJson := bytes.Compare(headers, []byte("{}")) == 0
	if headers == nil || len(headers) == 0 || emptyJson {
		return []sarama.RecordHeader{}, nil
	}

	h := map[string]interface{}{}

	dec := json.NewDecoder(bytes.NewBuffer(headers))
	dec.UseNumber()

	err := dec.Decode(&h)
	if err != nil {
		return nil, err
	}

	var recs []sarama.RecordHeader
	for k, v := range h {
		rec := sarama.RecordHeader{
			Key: []byte(k),
		}

		if str, ok := v.(string); ok {
			rec.Value = []byte(str)
		} else if num, ok := v.(json.Number); ok {
			rec.Value = []byte(string(num))
		}
		recs = append(recs, rec)
	}

	return recs, nil
}
