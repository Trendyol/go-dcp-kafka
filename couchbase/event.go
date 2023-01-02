package couchbase

type Event struct {
	Key       []byte
	Value     []byte
	IsDeleted bool
	IsExpired bool
	IsMutated bool
}

func NewDeleteEvent(key []byte, value []byte) Event {
	return Event{
		Key:       key,
		Value:     value,
		IsDeleted: true,
	}
}

func NewExpireEvent(key []byte, value []byte) Event {
	return Event{
		Key:       key,
		Value:     value,
		IsExpired: true,
	}
}

func NewMutateEvent(key []byte, value []byte) Event {
	return Event{
		Key:       key,
		Value:     value,
		IsMutated: true,
	}
}
