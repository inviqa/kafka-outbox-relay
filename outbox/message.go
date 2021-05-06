package outbox

import (
	"database/sql"

	"github.com/google/uuid"
)

type Batch struct {
	Id       uuid.UUID
	Messages []*Message
}

type Message struct {
	Id              uint
	BatchId         *uuid.UUID
	PushStartedAt   sql.NullTime
	PushCompletedAt sql.NullTime
	PayloadJson     []byte
	PayloadHeaders  []byte
	Topic           string
	PushAttempts    int
	Errored         bool
	ErrorReason     error
}
