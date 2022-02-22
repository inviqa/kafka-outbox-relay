package sql

import (
	"fmt"
	"strings"
)

type PostgresQueryProvider struct {
	Table   string
	Columns []string
}

func (m PostgresQueryProvider) MessagesSuccessUpdateSql(idCount int) string {
	q := `UPDATE %s SET push_completed_at = NOW(), error_reason = '', push_attempts = push_attempts + 1 WHERE id IN (%s)`

	var placeholders []string
	for i := 1; i <= idCount; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}

	return fmt.Sprintf(q, m.Table, strings.Join(placeholders, ", "))
}

func (m PostgresQueryProvider) MessageErroredUpdateSql(maxPushAttempts int) string {
	q := `UPDATE %s SET error_reason = $1, errored = CASE WHEN push_attempts + 1 >= %d THEN 1 ELSE 0 END, push_started_at = NULL, batch_id = NULL, push_attempts = push_attempts + 1 WHERE id = $2`

	return fmt.Sprintf(q, m.Table, maxPushAttempts)
}

func (m PostgresQueryProvider) BatchCreationSql(batchSize int) string {
	q := `UPDATE %s SET batch_id = $1, push_started_at = NOW()
		WHERE id IN(
			SELECT id FROM %s WHERE ((batch_id IS NULL AND push_started_at IS NULL) OR
		(batch_id IS NOT NULL AND push_completed_at IS NULL AND push_started_at < $2)) AND errored = $3 ORDERED BY created_at ASC LIMIT %d)`

	return fmt.Sprintf(q, m.Table, m.Table, batchSize)
}

func (m PostgresQueryProvider) BatchFetchSql() string {
	return fmt.Sprintf(`SELECT %s FROM %s WHERE batch_id = $1 ORDERED BY created_at ASC`, strings.Join(m.Columns, ", "), m.Table)
}

func (m PostgresQueryProvider) DeletePublishedMessagesSql() string {
	return fmt.Sprintf("DELETE FROM %s WHERE push_completed_at <= $1", m.Table)
}

func (m PostgresQueryProvider) GetQueueSizeSql() string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE push_completed_at IS NULL", m.Table)
}

func (m PostgresQueryProvider) GetTotalSizeSql() string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s", m.Table)
}
