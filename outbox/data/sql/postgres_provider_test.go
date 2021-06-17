package sql

import (
	"strings"
	"testing"
)

func TestPostgresQueryProvider_MessagesSuccessUpdateSql(t *testing.T) {
	actual := createPostgresProvider().MessagesSuccessUpdateSql(3)

	exp := `UPDATE kafka_outbox SET push_completed_at = NOW(), error_reason = '', push_attempts = push_attempts + 1 WHERE id IN ($1, $2, $3)`

	if actual != exp {
		t.Errorf(`received "%s" but expected "%s"`, actual, exp)
	}
}

func TestPostgresQueryProvider_BatchCreationSql(t *testing.T) {
	actual := createPostgresProvider().BatchCreationSql(20)

	if !strings.Contains(actual, "LIMIT 20") {
		t.Errorf("batch creation SQL does not contain the correct batch size limit")
	}
}

func TestPostgresQueryProvider_MessageErroredUpdateSql(t *testing.T) {
	actual := createPostgresProvider().MessageErroredUpdateSql(3)

	if !strings.Contains(actual, "errored = CASE WHEN push_attempts + 1 >= 3 THEN 1 ELSE 0 END") {
		t.Errorf("message errored SQL does not set the `errored` property as expected")
	}
}

func TestPostgresQueryProvider_DeletePublishedMessagesSql(t *testing.T) {
	actual := createPostgresProvider().DeletePublishedMessagesSql()

	if !strings.Contains(actual, "WHERE push_completed_at <= $1") {
		t.Errorf("delete SQL does not contain a valid constraint")
	}
}

func createPostgresProvider() *PostgresQueryProvider {
	return &PostgresQueryProvider{
		Columns: []string{"name", "foo"},
		Table:   "kafka_outbox",
	}
}
