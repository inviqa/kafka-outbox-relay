package outbox

import (
	"database/sql"
	"time"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/log"
	s "inviqa/kafka-outbox-relay/outbox/data/sql"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	ErrNoEvents = errors.New("no events in the batch")

	columns = []string{"id", "batch_id", "push_started_at", "push_completed_at", "topic", "payload_json", "payload_headers", "push_attempts", "key", "partition_key"}
)

type queryProvider interface {
	BatchCreationSql(batchSize int) string
	BatchFetchSql() string
	MessageErroredUpdateSql(maxPushAttempts int) string
	MessagesSuccessUpdateSql(idCount int) string
	DeletePublishedMessagesSql() string
	GetQueueSizeSql() string
	GetTotalSizeSql() string
}

type Repository struct {
	db            *sql.DB
	cfg           *config.Config
	queryProvider queryProvider
}

func NewRepository(db *sql.DB, cfg *config.Config) Repository {
	return NewRepositoryWithQueryProvider(db, cfg, newQueryProvider(cfg.DBDriver, cfg.DBOutboxTable, columns))
}

func NewRepositoryWithQueryProvider(db *sql.DB, cfg *config.Config, qp queryProvider) Repository {
	return Repository{
		db:            db,
		cfg:           cfg,
		queryProvider: qp,
	}
}

// GetBatch will create a new batch of records and then return them. It does
// so in a way that prevents any other processes picking up the same batch of
// events to avoid duplicate processing.
// If no events are created in the batch then the special ErrNoEvents value will
// be returned as the error.
func (r Repository) GetBatch() (*Batch, error) {
	batchId := uuid.New()
	stale := time.Now().In(time.UTC).Add(time.Duration(-10) * time.Minute) // TODO: make this configurable??

	upSql := r.queryProvider.BatchCreationSql(r.cfg.BatchSize)

	res, err := r.db.Exec(upSql, batchId, stale, 0)
	if err != nil {
		return nil, errors.Errorf("outbox: error creating a batch of events in repository: %s", err)
	}

	// if there is an error determining the affected rows, we treat it as a failed query
	// as the drivers we use never return an error value here
	count, _ := res.RowsAffected()
	if count < 1 {
		return nil, ErrNoEvents
	}

	rows, err := r.db.Query(r.queryProvider.BatchFetchSql(), batchId)
	if err != nil {
		return nil, errors.Errorf("outbox: error fetching created event batch in repository: %s", err)
	}

	batch := &Batch{
		Id:       batchId,
		Messages: []*Message{},
	}

	for rows.Next() {
		msg := &Message{}
		err := rows.Scan(&msg.Id, &msg.BatchId, &msg.PushStartedAt, &msg.PushCompletedAt, &msg.Topic, &msg.PayloadJson, &msg.PayloadHeaders, &msg.PushAttempts, &msg.Key, &msg.PartitionKey)
		if err != nil {
			return nil, errors.Errorf("outbox: error scanning event result into memory in repository: %s", err)
		}
		batch.Messages = append(batch.Messages, msg)
	}

	return batch, nil
}

func (r Repository) CommitBatch(batch *Batch) {
	log.Logger.WithFields(logrus.Fields{
		"batch_id":     batch.Id.String(),
		"num_messages": len(batch.Messages),
	}).Debug("starting batch commit")

	tx, err := r.db.Begin()
	if err != nil {
		log.Logger.Errorf("error occurred starting a DB transaction to commit the batch: %s", err)
		return
	}

	var successIds []interface{}
	for _, msg := range batch.Messages {
		if msg.ErrorReason != nil {
			r.updateErroredMessage(tx, msg)
		} else {
			successIds = append(successIds, msg.Id)
		}
	}

	if len(successIds) > 0 {
		err = r.updateSuccessfulMessages(tx, successIds)
		if err != nil {
			log.Logger.Errorf("error occurred updating successful outbox messages for batch ID %s: %s", batch.Id, err)
			err = tx.Rollback()
			if err != nil {
				log.Logger.Errorf("error rolling back the DB transaction: %s", err)
			}
			return
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Logger.Errorf("error occurred committing transaction for batch: %s", err)
	}
}

func (r Repository) DeletePublished(olderThan time.Time) (int64, error) {
	q := r.queryProvider.DeletePublishedMessagesSql()
	res, err := r.db.Exec(q, olderThan)

	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

func (r Repository) GetQueueSize() (uint, error) {
	q := r.queryProvider.GetQueueSizeSql()
	res := r.db.QueryRow(q)

	var count uint
	err := res.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (r Repository) GetTotalSize() (uint, error) {
	q := r.queryProvider.GetTotalSizeSql()
	res := r.db.QueryRow(q)

	var count uint
	err := res.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (r Repository) updateErroredMessage(tx *sql.Tx, msg *Message) {
	q := r.queryProvider.MessageErroredUpdateSql(r.cfg.KafkaPublishAttempts)
	_, err := tx.Exec(q, msg.ErrorReason.Error(), msg.Id)

	log.Logger.WithFields(logrus.Fields{"query": q, "error_reason": msg.ErrorReason, "id": msg.Id}).Debug("updating errored message")

	if err != nil {
		log.Logger.Errorf("error occurred updating the outbox message with ID %d: %s", msg.Id, err)
	}
}

func (r Repository) updateSuccessfulMessages(tx *sql.Tx, ids []interface{}) error {
	q := r.queryProvider.MessagesSuccessUpdateSql(len(ids))

	log.Logger.WithFields(logrus.Fields{"query": q, "ids": ids}).Debug("updating successful messages")

	_, err := tx.Exec(q, ids...)

	return err
}

func newQueryProvider(d config.DbDriver, table string, columns []string) queryProvider {
	switch true {
	case d.Postgres():
		return &s.PostgresQueryProvider{
			Table:   table,
			Columns: columns,
		}
	case d.MySQL():
		return &s.MysqlQueryProvider{
			Table:   table,
			Columns: columns,
		}
	}

	return nil
}
