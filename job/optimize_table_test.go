package job

import (
	"net/http"
	"reflect"
	"testing"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/job/test"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNewOptimizeTableWithDefaultClientForPostgres(t *testing.T) {
	db, _, _ := sqlmock.New()

	exp := &postgresOptimizeTable{
		Db:        db,
		TableName: "outbox",
		SidecarQuitter: SidecarQuitter{
			Client: http.DefaultClient,
		},
	}

	act := newOptimizeTableWithDefaultClient(db, "outbox", config.Postgres)
	if !reflect.DeepEqual(exp, act) {
		t.Error("expected mysqlOptimizeTable does not match actual")
	}
}

func TestNewOptimizeTableWithDefaultClientForMySQL(t *testing.T) {
	db, _, _ := sqlmock.New()

	exp := &mysqlOptimizeTable{
		Db:        db,
		TableName: "outbox",
		SidecarQuitter: SidecarQuitter{
			Client: http.DefaultClient,
		},
	}

	act := newOptimizeTableWithDefaultClient(db, "outbox", config.MySQL)
	if !reflect.DeepEqual(exp, act) {
		t.Error("expected mysqlOptimizeTable does not match actual")
	}
}

func TestNewOptimizeTableForPostgres(t *testing.T) {
	db, _, _ := sqlmock.New()
	cl := test.NewMockHttpClient()

	exp := &postgresOptimizeTable{
		Db:        db,
		TableName: "foo",
		SidecarQuitter: SidecarQuitter{
			Client: cl,
		},
	}

	act := newOptimizeTable(db, "foo", config.Postgres, cl)
	if !reflect.DeepEqual(exp, act) {
		t.Error("expected mysqlOptimizeTable does not match actual")
	}
}

func TestNewOptimizeTableForMySQL(t *testing.T) {
	db, _, _ := sqlmock.New()
	cl := test.NewMockHttpClient()

	exp := &mysqlOptimizeTable{
		Db:        db,
		TableName: "foo",
		SidecarQuitter: SidecarQuitter{
			Client: cl,
		},
	}

	act := newOptimizeTable(db, "foo", config.MySQL, cl)
	if !reflect.DeepEqual(exp, act) {
		t.Error("expected mysqlOptimizeTable does not match actual")
	}
}
