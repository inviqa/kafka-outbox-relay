package job

import (
	"errors"
	"inviqa/kafka-outbox-relay/job/test"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresOptimizeTable_Execute(t *testing.T) {
	db, mock, _ := sqlmock.New()
	mock.ExpectExec("VACUUM outbox;").WillReturnResult(sqlmock.NewResult(0, 0))

	j := &postgresOptimizeTable{
		Db:             db,
		TableName:      "outbox",
		SidecarQuitter: SidecarQuitter{},
	}
	err := j.Execute()

	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("some SQL expectations were not met: %s", err)
	}
}

func TestPostgresOptimizeTable_ExecuteWithError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	mock.ExpectExec("VACUUM outbox;").WillReturnError(errors.New("oops"))

	j := &postgresOptimizeTable{
		Db:             db,
		TableName:      "outbox",
		SidecarQuitter: SidecarQuitter{},
	}
	err := j.Execute()

	if err == nil {
		t.Error("expected an error but got nil")
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("some SQL expectations were not met: %s", err)
	}
}

func TestPostgresOptimizeTable_ExecuteWithSidecarProxyQuit(t *testing.T) {
	db, mock, _ := sqlmock.New()
	mock.ExpectExec("VACUUM outbox;").WillReturnResult(sqlmock.NewResult(0, 0))
	cl := test.NewMockHttpClient()
	j := &postgresOptimizeTable{
		Db:             db,
		TableName:      "outbox",
		SidecarQuitter: SidecarQuitter{Client: cl},
	}
	j.EnableSideCarProxyQuit("http://localhost:8000")
	err := j.Execute()

	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("some SQL expectations were not met: %s", err)
	}

	if len(cl.SentReqs) == 0 {
		t.Errorf("expected a call to sidecar proxy http://localhost:8000/quitquitquit, but there was none")
	}
}

func TestPostgresOptimizeTable_ExecuteWithSidecarProxyQuitClientError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	mock.ExpectExec("VACUUM outbox;").WillReturnResult(sqlmock.NewResult(0, 0))
	cl := test.NewMockHttpClient()
	cl.ReturnErrors()
	j := &postgresOptimizeTable{
		Db:             db,
		TableName:      "outbox",
		SidecarQuitter: SidecarQuitter{Client: cl},
	}
	j.EnableSideCarProxyQuit("http://localhost:8000")
	err := j.Execute()

	if err == nil {
		t.Error("expected an error but got nil")
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("some SQL expectations were not met: %s", err)
	}

	if len(cl.SentReqs) > 0 {
		t.Errorf("unexpected call to sidecar proxy http://localhost:8000/quitquitquit")
	}
}
