package job

import (
	"context"
	"errors"
	"testing"

	"inviqa/kafka-outbox-relay/job/test"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMysqlOptimizeTable_Execute(t *testing.T) {
	ctx := context.Background()
	db, mock, _ := sqlmock.New()
	mock.ExpectExec("OPTIMIZE TABLE outbox;").WillReturnResult(sqlmock.NewResult(0, 0))

	j := &mysqlOptimizeTable{
		Db:             db,
		TableName:      "outbox",
		SidecarQuitter: SidecarQuitter{},
	}
	err := j.Execute(ctx)

	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("some SQL expectations were not met: %s", err)
	}
}

func TestMysqlOptimizeTable_ExecuteWithError(t *testing.T) {
	ctx := context.Background()
	db, mock, _ := sqlmock.New()
	mock.ExpectExec("OPTIMIZE TABLE outbox;").WillReturnError(errors.New("oops"))

	j := &mysqlOptimizeTable{
		Db:             db,
		TableName:      "outbox",
		SidecarQuitter: SidecarQuitter{},
	}
	err := j.Execute(ctx)

	if err == nil {
		t.Error("expected an error but got nil")
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("some SQL expectations were not met: %s", err)
	}
}

func TestMysqlOptimizeTable_ExecuteWithSidecarProxyQuit(t *testing.T) {
	ctx := context.Background()
	db, mock, _ := sqlmock.New()
	mock.ExpectExec("OPTIMIZE TABLE outbox;").WillReturnResult(sqlmock.NewResult(0, 0))
	cl := test.NewMockHttpClient()
	j := &mysqlOptimizeTable{
		Db:             db,
		TableName:      "outbox",
		SidecarQuitter: SidecarQuitter{Client: cl},
	}
	j.EnableSideCarProxyQuit("http://localhost:8000")
	err := j.Execute(ctx)

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

func TestMysqlOptimizeTable_ExecuteWithSidecarProxyQuitClientError(t *testing.T) {
	ctx := context.Background()
	db, mock, _ := sqlmock.New()
	mock.ExpectExec("OPTIMIZE TABLE outbox;").WillReturnResult(sqlmock.NewResult(0, 0))
	cl := test.NewMockHttpClient()
	cl.ReturnErrors()
	j := &mysqlOptimizeTable{
		Db:             db,
		TableName:      "outbox",
		SidecarQuitter: SidecarQuitter{Client: cl},
	}
	j.EnableSideCarProxyQuit("http://localhost:8000")
	err := j.Execute(ctx)

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
