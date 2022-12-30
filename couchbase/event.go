package couchbase

import "sync"

type Event struct {
	Key       []byte
	Value     []byte
	IsDeleted bool
	IsExpired bool
	IsMutated bool
}

func NewDeleteEvent(key []byte, value []byte) *Event {
	event := getEvent(key, value)
	event.IsDeleted = true
	return event
}

func NewExpireEvent(key []byte, value []byte) *Event {
	event := getEvent(key, value)
	event.IsExpired = true
	return event
}

func NewMutateEvent(key []byte, value []byte) *Event {
	event := getEvent(key, value)
	event.IsMutated = true
	return event
}

func getEvent(key []byte, value []byte) *Event {
	event := CbDcpEventPool.Get().(*Event)
	event.Key = key
	event.Value = value
	return event
}

var CbDcpEventPool = sync.Pool{
	New: func() any {
		return &Event{}
	},
}
