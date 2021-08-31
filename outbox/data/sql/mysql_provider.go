package sql

import (
	"fmt"
	"strings"
)

type MysqlQueryProvider struct {
	Table   string
	Columns []string
}

func (m MysqlQueryProvider) MessagesSuccessUpdateSql(idCount int) string {
	q := `UPDATE %s SET push_completed_at = NOW(), error_reason = "", push_attempts = push_attempts + 1 WHERE id IN (%s)`

	return fmt.Sprintf(q, m.Table, strings.Trim(strings.Repeat("?, ", idCount), ", "))
}

func (m MysqlQueryProvider) MessageErroredUpdateSql(maxPushAttempts int) string {
	q := "UPDATE `%s` SET `error_reason` = ?, `errored` = IF((`push_attempts` + 1) >= %d, 1, 0), `push_started_at` = NULL, `batch_id` = NULL, `push_attempts` = `push_attempts` + 1 WHERE `id` = ?"

	return fmt.Sprintf(q, m.Table, maxPushAttempts)
}

func (m MysqlQueryProvider) BatchCreationSql(batchSize int) string {
	q := `UPDATE %s SET batch_id = ?, push_started_at = NOW()
		WHERE ((batch_id IS NULL AND push_started_at IS NULL) OR
		(batch_id IS NOT NULL AND push_completed_at IS NULL AND push_started_at < ?) AND errored = ?) LIMIT %d`

	return fmt.Sprintf(q, m.Table, batchSize)
}

func (m MysqlQueryProvider) BatchFetchSql() string {
	return fmt.Sprintf(`SELECT %s FROM %s WHERE batch_id = ?`, strings.Join(m.escapeColumns(), ", "), m.Table)
}

func (m MysqlQueryProvider) DeletePublishedMessagesSql() string {
	return fmt.Sprintf("DELETE FROM %s WHERE push_completed_at <= ?", m.Table)
}

func (m MysqlQueryProvider) GetQueueSizeSql() string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE push_completed_at IS NULL", m.Table)
}

func (m MysqlQueryProvider) GetTotalSizeSql() string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s", m.Table)
}

func (m MysqlQueryProvider) escapeColumns() []string {
	var escaped []string
	for _, c := range m.Columns {
		escaped = append(escaped, "`"+c+"`")
	}

	return escaped
}
