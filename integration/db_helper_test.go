//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"inviqa/kafka-outbox-relay/outbox"
)

func ensureOutboxTableExists() {
	var q string
	if dbCfg.Driver.MySQL() {
		q = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s LIKE kafka_outbox;", dbCfg.OutboxTable)
	} else {
		q = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (LIKE kafka_outbox INCLUDING ALL);", dbCfg.OutboxTable)
	}
	_, err := db.Exec(q)
	if err != nil {
		panic(fmt.Sprintf("an error occurred creating the outbox table for integration tests: %s", err))
	}
}

func purgeOutboxTable() {
	_, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s;", dbCfg.OutboxTable))
	if err != nil {
		panic(fmt.Sprintf("an error occurred cleaning the outbox table for tests: %s", err))
	}
}

func insertOutboxMessages(msgs []*outbox.Message) {
	tx, err := db.Begin()
	if err != nil {
		panic(fmt.Sprintf("error creating a DB transaction: %s", err))
	}

	for _, msg := range msgs {
		if msg.PayloadHeaders == nil {
			msg.PayloadHeaders = []byte("{}")
		}

		var q string
		var err error
		var id int64
		if dbCfg.Driver.MySQL() {
			q = fmt.Sprintf("INSERT INTO `%s` SET batch_id = ?, topic = ?, push_started_at = ?, push_completed_at = ?, payload_json = ?, payload_headers = ?, push_attempts = ?, `key` = ?, partition_key = ?;", dbCfg.OutboxTable)
			res, err := tx.Exec(q, msg.BatchId, msg.Topic, msg.PushStartedAt, msg.PushCompletedAt, msg.PayloadJson, msg.PayloadHeaders, msg.PushAttempts, msg.Key, msg.PartitionKey)
			if err != nil {
				panic(fmt.Sprintf("failed to insert outbox message in MySQL: %s", err))
			}

			id, err = res.LastInsertId()
			if err != nil {
				panic(fmt.Sprintf("failed to determine last insert ID for the inserted outbox message: %s", err))
			}
		} else {
			q = fmt.Sprintf("INSERT INTO %s(batch_id, topic, push_started_at, push_completed_at, payload_json, payload_headers, push_attempts, key, partition_key) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id;", dbCfg.OutboxTable)
			err = tx.QueryRow(q, msg.BatchId, msg.Topic, msg.PushStartedAt, msg.PushCompletedAt, msg.PayloadJson, msg.PayloadHeaders, msg.PushAttempts, msg.Key, msg.PartitionKey).Scan(&id)
			if err != nil {
				panic(fmt.Sprintf("failed to insert outbox message in Postgres: %s", err))
			}
		}
		msg.Id = uint(id)
	}

	err = tx.Commit()
	if err != nil {
		panic(fmt.Sprintf("error committing DB transaction: %s", err))
	}
}

func getOutboxMessage(id uint) *outbox.Message {
	q := fmt.Sprintf("SELECT id, batch_id, push_started_at, push_completed_at, topic, payload_json, payload_headers, push_attempts, errored, error_reason FROM %s WHERE id = ?", dbCfg.OutboxTable)
	if dbCfg.Driver.Postgres() {
		q = strings.Replace(q, "?", "$1", 1)
	}

	res := &outbox.Message{}
	var errReason string
	row := db.QueryRow(q, id)
	err := row.Scan(&res.Id, &res.BatchId, &res.PushStartedAt, &res.PushCompletedAt, &res.Topic, &res.PayloadJson, &res.PayloadHeaders, &res.PushAttempts, &res.Errored, &errReason)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			panic(fmt.Sprintf("no outbox message records found with ID %d", id))
		}
		panic(fmt.Sprintf("an error occurred scanning the outbox message: %s", err))
	}

	if errReason != "" {
		res.ErrorReason = errors.New(errReason)
	}

	return res
}

func outboxMessageExists(id uint) bool {
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = ?", dbCfg.OutboxTable)
	if dbCfg.Driver.Postgres() {
		q = strings.Replace(q, "?", "$1", 1)
	}

	var count int
	res := db.QueryRow(q, id)
	if err := res.Scan(&count); err != nil {
		panic(err)
	}

	return count > 0
}
