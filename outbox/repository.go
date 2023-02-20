package outbox

import (
	"context"
	"database/sql"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox/data"
	s "inviqa/kafka-outbox-relay/outbox/data/sql"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	ErrNoEvents = errors.New("no events in the batch")

	columns = []string{"id", "batch_id", "push_started_at", "push_completed_at", "topic", "payload_json", "payload_headers", "push_attempts", "key", "partition_key"}
)

const (
	Insert Operation = "INSERT"
	Update Operation = "UPDATE"
	Delete Operation = "DELETE"
	Select Operation = "SELECT"
)

type Operation string

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
	dbCfg         config.Database
	queryProvider queryProvider
}

func NewRepository(db data.DB, cfg *config.Config) Repository {
	dbCfg := db.Config()

	return NewRepositoryWithQueryProvider(
		db.Connection(),
		cfg,
		dbCfg,
		newQueryProvider(dbCfg.Driver, dbCfg.OutboxTable, columns),
	)
}

func NewRepositoryWithQueryProvider(db *sql.DB, cfg *config.Config, dbCfg config.Database, qp queryProvider) Repository {
	return Repository{
		db:            db,
		cfg:           cfg,
		dbCfg:         dbCfg,
		queryProvider: qp,
	}
}

// GetBatch will create a new batch of records and then return them. It does
// so in a way that prevents any other processes picking up the same batch of
// events to avoid duplicate processing.
// If no events are created in the batch then the special ErrNoEvents value will
// be returned as the error.
func (r Repository) GetBatch(ctx context.Context) (*Batch, error) {
	defer newrelic.FromContext(ctx).StartSegment("outbox: Repository.GetBatch()").End()

	batchId := uuid.New()
	stale := time.Now().In(time.UTC).Add(time.Duration(-10) * time.Minute) // TODO: make this configurable??

	upSql := r.queryProvider.BatchCreationSql(r.cfg.BatchSize)

	res, err := r.execContext(ctx, upSql, Update, batchId, stale, 0)
	if err != nil {
		return nil, errors.Errorf("outbox: error creating a batch of events in repository: %s", err)
	}

	// the drivers we use never return an error value here
	count, _ := res.RowsAffected()
	if count < 1 {
		return nil, ErrNoEvents
	}

	rows, err := r.queryContext(ctx, r.queryProvider.BatchFetchSql(), batchId)
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

func (r Repository) CommitBatch(ctx context.Context, batch *Batch) {
	defer newrelic.FromContext(ctx).StartSegment("outbox: Repository.CommitBatch()").End()

	log.Logger.WithFields(logrus.Fields{
		"batch_id":     batch.Id.String(),
		"num_messages": len(batch.Messages),
	}).Debug("starting batch commit")

	tx, err := r.db.Begin()
	if err != nil {
		log.Logger.Errorf("error occurred starting a DB transaction to commit the batch: %s", err)
		return
	}

	var successIds []any
	for _, msg := range batch.Messages {
		if msg.ErrorReason != nil {
			r.updateErroredMessage(ctx, tx, msg)
		} else {
			successIds = append(successIds, msg.Id)
		}
	}

	if len(successIds) > 0 {
		err = r.updateSuccessfulMessages(ctx, tx, successIds)
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

func (r Repository) DeletePublished(ctx context.Context, olderThan time.Time) (int64, error) {
	defer newrelic.FromContext(ctx).StartSegment("outbox: Repository.DeletePublished()").End()

	q := r.queryProvider.DeletePublishedMessagesSql()
	res, err := r.execContext(ctx, q, Delete, olderThan)

	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

func (r Repository) GetQueueSize(ctx context.Context) (uint, error) {
	q := r.queryProvider.GetQueueSizeSql()
	res := r.queryRowContext(ctx, q)

	var count uint
	err := res.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (r Repository) GetTotalSize(ctx context.Context) (uint, error) {
	q := r.queryProvider.GetTotalSizeSql()
	res := r.queryRowContext(ctx, q)

	var count uint
	err := res.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (r Repository) updateErroredMessage(ctx context.Context, tx *sql.Tx, msg *Message) {
	q := r.queryProvider.MessageErroredUpdateSql(r.cfg.KafkaPublishAttempts)
	_, err := r.execContextWithTx(ctx, tx, q, Update, msg.ErrorReason.Error(), msg.Id)

	log.Logger.WithFields(logrus.Fields{"query": q, "error_reason": msg.ErrorReason, "id": msg.Id}).Debug("updating errored message")

	if err != nil {
		log.Logger.Errorf("error occurred updating the outbox message with ID %d: %s", msg.Id, err)
	}
}

func (r Repository) updateSuccessfulMessages(ctx context.Context, tx *sql.Tx, ids []interface{}) error {
	q := r.queryProvider.MessagesSuccessUpdateSql(len(ids))

	log.Logger.WithFields(logrus.Fields{"query": q, "ids": ids}).Debug("updating successful messages")

	_, err := r.execContextWithTx(ctx, tx, q, Update, ids...)

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

func (r Repository) execContext(ctx context.Context, sql string, operation Operation, args ...interface{}) (sql.Result, error) {
	ds := r.dataStoreSegment(ctx, operation)
	defer ds.End()

	return r.db.ExecContext(ctx, sql, args...)
}

func (r Repository) execContextWithTx(ctx context.Context, tx *sql.Tx, sql string, operation Operation, args ...interface{}) (sql.Result, error) {
	ds := r.dataStoreSegment(ctx, operation)
	defer ds.End()

	return tx.ExecContext(ctx, sql, args...)
}

func (r Repository) queryContext(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	ds := r.dataStoreSegment(ctx, Select)
	defer ds.End()

	return r.db.QueryContext(ctx, sql, args...)
}

func (r Repository) queryRowContext(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	ds := r.dataStoreSegment(ctx, Select)
	defer ds.End()

	return r.db.QueryRowContext(ctx, sql, args...)
}

func (r Repository) dataStoreSegment(ctx context.Context, operation Operation) newrelic.DatastoreSegment {
	return newrelic.DatastoreSegment{
		Product:    r.dbCfg.Driver.NewRelicType(),
		Collection: r.dbCfg.OutboxTable,
		Operation:  string(operation),
		StartTime:  newrelic.FromContext(ctx).StartSegmentNow(),
	}
}
