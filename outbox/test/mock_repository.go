package test

import (
	"errors"
	"reflect"
	"sync"
	"time"

	"inviqa/kafka-outbox-relay/outbox"
)

type MockRepository struct {
	sync.RWMutex
	getBatchCallCount   int
	mockQueueSize       uint
	mockTotalSize       uint
	batchesToReturn     []*outbox.Batch
	committed           map[*outbox.Batch]bool
	batchesCommitted    []*outbox.Batch
	returnError         bool
	deletedRowsCount    int64
	returnNoEventsError bool
}

func NewMockRepository() *MockRepository {
	return &MockRepository{
		batchesToReturn: []*outbox.Batch{},
		committed:       map[*outbox.Batch]bool{},
	}
}

func (mr *MockRepository) GetBatch() (*outbox.Batch, error) {
	mr.RLock()
	defer mr.RUnlock()
	mr.getBatchCallCount++

	if mr.returnNoEventsError {
		return nil, outbox.ErrNoEvents
	}

	if mr.returnError {
		return nil, errors.New("oops")
	}


	return mr.popBatch(), nil
}

func (mr *MockRepository) CommitBatch(batch *outbox.Batch) {
	mr.Lock()
	defer mr.Unlock()
	mr.batchesCommitted = append(mr.batchesCommitted, batch)
	mr.committed[batch] = true
}

func (mr *MockRepository) AddBatch(batch *outbox.Batch) {
	mr.Lock()
	defer mr.Unlock()
	mr.batchesToReturn = append(mr.batchesToReturn, batch)
}

func (mr *MockRepository) BatchWasCommitted(batch *outbox.Batch) bool {
	mr.RLock()
	defer mr.RUnlock()
	if ok := mr.committed[batch]; !ok {
		return false
	}
	return true
}

func (mr *MockRepository) GetCommittedBatch(batch *outbox.Batch) *outbox.Batch {
	mr.RLock()
	defer mr.RUnlock()
	if !mr.BatchWasCommitted(batch) {
		return nil
	}

	for _, b := range mr.batchesCommitted {
		if reflect.DeepEqual(b, batch) {
			return b
		}
	}

	return nil
}

func (mr *MockRepository) DeletePublished(olderThan time.Time) (int64, error) {
	if mr.returnError {
		return 0, errors.New("oops")
	}
	return mr.deletedRowsCount, nil
}

func (mr *MockRepository) GetQueueSize() (uint, error) {
	if mr.returnError {
		return 0, errors.New("oops")
	}

	return mr.mockQueueSize, nil
}

func (mr *MockRepository) GetTotalSize() (uint, error) {
	if mr.returnError {
		return 0, errors.New("oops")
	}

	return mr.mockTotalSize, nil
}

func (mr *MockRepository) GetBatchCallCount() int {
	return mr.getBatchCallCount
}

func (mr *MockRepository) ReturnErrors() {
	mr.returnError = true
}

func (mr *MockRepository) ReturnNoEventsError() {
	mr.returnNoEventsError = true
}

func (mr *MockRepository) SetQueueSize(size uint) {
	mr.mockQueueSize = size
}

func (mr *MockRepository) SetTotalSize(size uint) {
	mr.mockTotalSize = size
}

func (mr *MockRepository) popBatch() *outbox.Batch {
	if len(mr.batchesToReturn) == 0 {
		return nil
	}

	var b *outbox.Batch
	b, mr.batchesToReturn = mr.batchesToReturn[0], mr.batchesToReturn[1:]

	return b
}

func (mr *MockRepository) SetDeletedRowsCount(c int64) {
	mr.deletedRowsCount = c
}
