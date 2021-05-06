package sql

import (
	"strings"
	"testing"
)

func TestMysqlQueryProvider_MessagesSuccessUpdateSql(t *testing.T) {
	actual := createProvider().MessagesSuccessUpdateSql(3)

	exp := `UPDATE kafka_outbox SET push_completed_at = NOW(), error_reason = "", push_attempts = push_attempts + 1 WHERE id IN (?, ?, ?)`

	if actual != exp {
		t.Errorf(`received "%s" but expected "%s"`, actual, exp)
	}
}

func TestMysqlQueryProvider_BatchCreationSql(t *testing.T) {
	actual := createProvider().BatchCreationSql(20)

	if !strings.Contains(actual, "LIMIT 20") {
		t.Errorf("batch creation SQL does not contain the correct batch size limit")
	}
}

func TestMysqlQueryProvider_MessageErroredUpdateSql(t *testing.T) {
	actual := createProvider().MessageErroredUpdateSql(10)

	if !strings.Contains(actual, "`errored` = IF((`push_attempts` + 1) >= 10, 1, 0)") {
		t.Errorf("message errored SQL does not set the `errored` property as expected")
	}
}

func TestMysqlQueryProvider_DeletePublishedMessagesSql(t *testing.T) {
	actual := createProvider().DeletePublishedMessagesSql()

	if !strings.Contains(actual, "WHERE push_completed_at <= ?") {
		t.Errorf("delete SQL does not contain a valid constraint")
	}
}

func createProvider() *MysqlQueryProvider {
	return &MysqlQueryProvider{
		Columns: []string{"name", "foo"},
		Table:   "kafka_outbox",
	}
}
