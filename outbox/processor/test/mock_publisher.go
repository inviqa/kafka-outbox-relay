package test

import (
	"errors"
	"inviqa/kafka-outbox-relay/outbox"
	"reflect"
	"sync"
)

type mockPublisher struct {
	sync.RWMutex
	publishedMessages []*outbox.Message
	errors            map[*outbox.Message]error
}

func NewMockPublisher() *mockPublisher {
	return &mockPublisher{
		publishedMessages: []*outbox.Message{},
		errors:            map[*outbox.Message]error{},
	}
}

func (p *mockPublisher) PublishMessage(m *outbox.Message) error {
	p.Lock()
	defer p.Unlock()
	if err, ok := p.errors[m]; ok {
		return err
	}

	p.publishedMessages = append(p.publishedMessages, m)

	return nil
}

func (p *mockPublisher) MessageWasPublished(exp *outbox.Message) bool {
	p.RLock()
	defer p.RUnlock()
	for _, m := range p.publishedMessages {
		if reflect.DeepEqual(m, exp) {
			return true
		}
	}

	return false
}

func (p *mockPublisher) ErrorForMessage(m *outbox.Message) {
	p.Lock()
	defer p.Unlock()
	p.errors[m] = errors.New("foo")
}

func (p *mockPublisher) Close() error {
	return nil
}
