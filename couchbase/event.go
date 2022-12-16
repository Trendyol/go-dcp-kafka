package couchbase

type Event struct {
	Key       string
	Value     *string
	IsDeleted bool
	IsExpired bool
	IsMutated bool
}

func NewDeleteEvent(key string, value *string) *Event {
	return &Event{Key: key, Value: value, IsDeleted: true}
}

func NewExpireEvent(key string, value *string) *Event {
	return &Event{Key: key, Value: value, IsExpired: true}
}

func NewMutateEvent(key string, value *string) *Event {
	return &Event{Key: key, Value: value, IsMutated: true}
}
