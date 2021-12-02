package outbox

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/outbox/data"
	s "inviqa/kafka-outbox-relay/outbox/data/sql"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-test/deep"
	"github.com/google/uuid"
)

func TestNewRepository(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	dbConn, _, _ := sqlmock.New()

	tests := []struct {
		name             string
		cfg              *config.Config
		dbCfg            config.Database
		driver           config.DbDriver
		expQueryProvider queryProvider
	}{
		{
			name: "mysql query provider",
			dbCfg: config.Database{
				OutboxTable: "outbox_table",
				Driver:      config.MySQL,
			},
			expQueryProvider: &s.MysqlQueryProvider{Table: "outbox_table", Columns: columns},
		},
		{
			name: "postgres query provider",
			dbCfg: config.Database{
				OutboxTable: "outbox_table",
				Driver:      config.Postgres,
			},
			expQueryProvider: &s.PostgresQueryProvider{Table: "outbox_table", Columns: columns},
		},
	}

	for _, tt := range tests {
		db := data.NewDB(dbConn, tt.dbCfg)
		t.Run(tt.name, func(t *testing.T) {
			exp := Repository{
				db:            dbConn,
				cfg:           tt.cfg,
				queryProvider: tt.expQueryProvider,
			}

			got := NewRepository(db, tt.cfg)
			if diff := deep.Equal(exp, got); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestNewRepositoryWithQueryProvider(t *testing.T) {
	deep.CompareUnexportedFields = true
	defer func() {
		deep.CompareUnexportedFields = false
	}()

	db, _, _ := sqlmock.New()
	cfg := &config.Config{}
	prov := &mockQueryProvider{}

	exp := Repository{
		db:            db,
		cfg:           cfg,
		queryProvider: prov,
	}

	got := NewRepositoryWithQueryProvider(db, cfg, prov)
	if diff := deep.Equal(exp, got); diff != nil {
		t.Error(diff)
	}
}

//gocyclo:ignore
func TestRepository_GetBatch(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	now := time.Now()
	now2 := now.Add(time.Second * 1)
	repo := NewRepositoryWithQueryProvider(db, &config.Config{BatchSize: 100}, &mockQueryProvider{})

	msgBatchId := uuid.MustParse("f58e7c8a-e0d2-47fb-8111-eb0ae02ea21e")
	rows := sqlmock.NewRows(columns).
		AddRow(123, msgBatchId, now, now2, "event.product", "foo", "{}", 0, "key-0", "partition-key-0").
		AddRow(124, msgBatchId, now, now2, "event.price", "bar", "{}", 1, "key-1", "partition-key-1")

	t.Run("it gets a batch of events", func(t *testing.T) {
		mock.ExpectExec(`UPDATE outbox LIMIT 100`).
			WillReturnResult(sqlmock.NewResult(1, 2))

		mock.ExpectQuery("SELECT.* FROM outbox").WillReturnRows(rows)

		got, err := repo.GetBatch()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("some SQL expectations were not met: %s", err)
		}

		if got.Id.String() == "" {
			t.Error("empty batch ID received")
		}

		exp := getExpectedMessageBatchForTest(msgBatchId, now, now2)
		exp.Id = got.Id
		if diff := deep.Equal(exp, got); diff != nil {
			t.Error(diff)
		}
	})

	t.Run("returns special error when no events found", func(t *testing.T) {
		mock.ExpectExec(`UPDATE outbox LIMIT 100`).
			WillReturnResult(sqlmock.NewResult(1, 0))

		_, err := repo.GetBatch()
		if !errors.Is(err, ErrNoEvents) {
			t.Fatalf("expected error '%s' but got '%s'", ErrNoEvents, err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("some SQL expectations were not met: %s", err)
		}
	})

	t.Run("it handles error creating a batch", func(t *testing.T) {
		mock.ExpectExec(`UPDATE outbox LIMIT 100`).
			WillReturnError(errors.New("oops"))

		_, err := repo.GetBatch()
		if err == nil {
			t.Error("expected an error but got nil")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("some SQL expectations were not met: %s", err)
		}
	})

	t.Run("it handles error getting the created batch", func(t *testing.T) {
		mock.ExpectExec(`UPDATE outbox LIMIT 100`).
			WillReturnResult(sqlmock.NewResult(1, 2))

		mock.ExpectQuery("SELECT.* FROM outbox").WillReturnError(errors.New("oops"))

		_, err := repo.GetBatch()
		if err == nil {
			t.Error("expected an error but got nil")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("some SQL expectations were not met: %s", err)
		}
	})
}

func TestRepository_CommitBatch(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	batchId := uuid.New()
	batch := createMockBatch(batchId)

	repo := NewRepositoryWithQueryProvider(db, &config.Config{}, &mockQueryProvider{})

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE outbox SET error_reason =.* WHERE id =.*").
		WithArgs(batch.Messages[1].ErrorReason.Error(), batch.Messages[1].Id).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec("UPDATE outbox SET push_completed_at =.* WHERE id IN.*").
		WithArgs(batch.Messages[0].Id, batch.Messages[2].Id).
		WillReturnResult(sqlmock.NewResult(0, 2))

	mock.ExpectCommit()

	repo.CommitBatch(batch)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("some SQL expectations were not met: %s", err)
	}
}

func TestRepository_CommitBatchWithTransactionCreateError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	repo := NewRepositoryWithQueryProvider(db, &config.Config{}, &mockQueryProvider{})

	mock.ExpectBegin().WillReturnError(errors.New("oops"))
	repo.CommitBatch(&Batch{Id: uuid.New(), Messages: []*Message{}})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("some SQL expectations were not met: %s", err)
	}
}

func TestRepository_CommitBatchWithErroredMessageUpdateQueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	repo := NewRepositoryWithQueryProvider(db, &config.Config{}, &mockQueryProvider{})

	batchId := uuid.New()
	batch := createMockBatch(batchId)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE outbox SET error_reason =.* WHERE id =.*").
		WithArgs(batch.Messages[1].ErrorReason.Error(), batch.Messages[1].Id).
		WillReturnError(errors.New("oops"))

	mock.ExpectExec("UPDATE outbox SET push_completed_at =.* WHERE id IN.*").
		WithArgs(batch.Messages[0].Id, batch.Messages[2].Id).
		WillReturnResult(sqlmock.NewResult(0, 2))

	mock.ExpectCommit()

	repo.CommitBatch(batch)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("some SQL expectations were not met: %s", err)
	}
}

func TestRepository_CommitBatchWithSuccessfulMessageUpdateQueryError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	repo := NewRepositoryWithQueryProvider(db, &config.Config{}, &mockQueryProvider{})

	batchId := uuid.New()
	batch := createMockBatchOfSuccessfulMessagesOnly(batchId)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE outbox SET push_completed_at =.* WHERE id IN.*").
		WithArgs(batch.Messages[0].Id, batch.Messages[1].Id).
		WillReturnError(errors.New("oops"))

	mock.ExpectRollback()

	repo.CommitBatch(batch)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("some SQL expectations were not met: %s", err)
	}
}

func TestRepository_DeletePublished(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	repo := NewRepositoryWithQueryProvider(db, &config.Config{}, &mockQueryProvider{})

	now := time.Now()
	mock.ExpectExec("DELETE FROM outbox WHERE push_completed_at <=.*").
		WithArgs(now).
		WillReturnResult(sqlmock.NewResult(0, 100))

	affRows, err := repo.DeletePublished(now)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if affRows != 100 {
		t.Errorf("expected 100 affected rows, but got %d", affRows)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("some SQL expectations were not met: %s", err)
	}
}

func TestRepository_DeletePublishedWithError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	repo := NewRepositoryWithQueryProvider(db, &config.Config{}, &mockQueryProvider{})

	now := time.Now()
	mock.ExpectExec("DELETE FROM outbox WHERE push_completed_at <=.*").
		WithArgs(now).
		WillReturnError(errors.New("oops"))

	affRows, err := repo.DeletePublished(now)
	if err == nil {
		t.Fatal("expected an error but got nil")
	}

	if affRows != 0 {
		t.Errorf("expected 0 affected rows, but got %d", affRows)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("some SQL expectations were not met: %s", err)
	}
}

func TestRepository_GetQueueSize(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	rows := sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(10)
	repo := NewRepositoryWithQueryProvider(db, &config.Config{}, &mockQueryProvider{})
	mock.ExpectQuery("SELECT COUNT.*WHERE.*").
		WillReturnRows(rows)

	size, err := repo.GetQueueSize()
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if size != 10 {
		t.Errorf("expected the queue size to be 10, but got %d", size)
	}
}

func TestRepository_GetTotalSize(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	rows := sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(99)
	repo := NewRepositoryWithQueryProvider(db, &config.Config{}, &mockQueryProvider{})
	mock.ExpectQuery("SELECT COUNT.*").
		WillReturnRows(rows)

	size, err := repo.GetTotalSize()
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if size != 99 {
		t.Errorf("expected the total size to be 10, but got %d", size)
	}
}

func createMockBatch(batchId uuid.UUID) *Batch {
	return &Batch{
		Id: batchId,
		Messages: []*Message{
			{
				Id:      1,
				BatchId: &batchId,
				PushStartedAt: sql.NullTime{
					Time:  time.Now(),
					Valid: true,
				},
				PushCompletedAt: sql.NullTime{},
				PayloadJson:     []byte(`1`),
				PayloadHeaders:  []byte(`headers-1`),
				Topic:           "productUpdate",
				PushAttempts:    1,
				Errored:         false,
				ErrorReason:     nil,
			},
			{
				Id:      2,
				BatchId: &batchId,
				PushStartedAt: sql.NullTime{
					Time:  time.Now(),
					Valid: true,
				},
				PushCompletedAt: sql.NullTime{},
				PayloadJson:     []byte(`2`),
				PayloadHeaders:  []byte(`headers-2`),
				Topic:           "productUpdate",
				PushAttempts:    0,
				Errored:         true,
				ErrorReason:     errors.New("something bad happened for number 2"),
			},
			{
				Id:      3,
				BatchId: &batchId,
				PushStartedAt: sql.NullTime{
					Time:  time.Now(),
					Valid: true,
				},
				PushCompletedAt: sql.NullTime{},
				PayloadJson:     []byte(`3`),
				PayloadHeaders:  []byte(`headers-3`),
				Topic:           "productUpdate",
				PushAttempts:    2,
				Errored:         false,
				ErrorReason:     nil,
			},
		},
	}
}

func createMockBatchOfSuccessfulMessagesOnly(batchId uuid.UUID) *Batch {
	batch := createMockBatch(batchId)
	var successfulMsgs []*Message
	for _, m := range batch.Messages {
		if !m.Errored {
			successfulMsgs = append(successfulMsgs, m)
		}
	}

	batch.Messages = successfulMsgs
	return batch
}

func getExpectedMessageBatchForTest(batchId uuid.UUID, pushStarted time.Time, pushCompleted time.Time) *Batch {
	return &Batch{
		Id: batchId,
		Messages: []*Message{
			{
				Id:      123,
				BatchId: &batchId,
				PushStartedAt: sql.NullTime{
					Time:  pushStarted,
					Valid: true,
				},
				PushCompletedAt: sql.NullTime{
					Time:  pushCompleted,
					Valid: true,
				},
				PayloadJson:    []byte("foo"),
				PayloadHeaders: []byte("{}"),
				Topic:          "event.product",
				Key:            "key-0",
				PartitionKey:   "partition-key-0",
			},
			{
				Id:      124,
				BatchId: &batchId,
				PushStartedAt: sql.NullTime{
					Time:  pushStarted,
					Valid: true,
				},
				PushCompletedAt: sql.NullTime{
					Time:  pushCompleted,
					Valid: true,
				},
				PayloadJson:    []byte("bar"),
				PayloadHeaders: []byte("{}"),
				PushAttempts:   1,
				Topic:          "event.price",
				Key:            "key-1",
				PartitionKey:   "partition-key-1",
			},
		},
	}
}

type mockQueryProvider struct {
}

func (m mockQueryProvider) MessagesSuccessUpdateSql(idCount int) string {
	return "UPDATE outbox SET push_completed_at = NOW() WHERE id IN (?)"
}

func (m mockQueryProvider) BatchCreationSql(batchSize int) string {
	return fmt.Sprintf("UPDATE outbox LIMIT %d", batchSize)
}

func (m mockQueryProvider) BatchFetchSql() string {
	return fmt.Sprintf("SELECT %s FROM outbox", columns)
}

func (m mockQueryProvider) MessageErroredUpdateSql(maxPushAttempts int) string {
	return "UPDATE outbox SET error_reason = ? WHERE id = ?"
}

func (m mockQueryProvider) DeletePublishedMessagesSql() string {
	return "DELETE FROM outbox WHERE push_completed_at <= ?"
}

func (m mockQueryProvider) GetQueueSizeSql() string {
	return "SELECT COUNT(*) FROM outbox WHERE push_completed_at IS NULL"
}

func (m mockQueryProvider) GetTotalSizeSql() string {
	return "SELECT COUNT(*) FROM outbox"
}
