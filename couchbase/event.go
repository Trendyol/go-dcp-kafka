package couchbase

import (
	"time"

	"github.com/Trendyol/go-dcp/tracing"
	"github.com/couchbase/gocbcore/v10"
)

type Event struct {
	tracing.ListenerTrace
	CollectionName string
	EventTime      time.Time
	Key            []byte
	Value          []byte
	Cas            uint64
	VbID           uint16
	IsDeleted      bool
	IsExpired      bool
	IsMutated      bool
	SeqNo          uint64
	RevNo          uint64
	VbUuid         gocbcore.VbUUID
}

func NewDeleteEvent(
	key []byte, value []byte,
	collectionName string, eventTime time.Time, cas uint64, vbID uint16, seqNo uint64, revNo uint64, vbUuid gocbcore.VbUUID,
) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsDeleted:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		Cas:            cas,
		VbID:           vbID,
		SeqNo:          seqNo,
		RevNo:          revNo,
		VbUuid:         vbUuid,
	}
}

func NewExpireEvent(
	key []byte, value []byte,
	collectionName string, eventTime time.Time, cas uint64, vbID uint16, seqNo uint64, revNo uint64, vbUuid gocbcore.VbUUID,
) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsExpired:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		Cas:            cas,
		VbID:           vbID,
		SeqNo:          seqNo,
		RevNo:          revNo,
		VbUuid:         vbUuid,
	}
}

func NewMutateEvent(
	key []byte, value []byte,
	collectionName string, eventTime time.Time, cas uint64, vbID uint16, seqNo uint64, revNo uint64, vbUuid gocbcore.VbUUID,
) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsMutated:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		Cas:            cas,
		VbID:           vbID,
		SeqNo:          seqNo,
		RevNo:          revNo,
		VbUuid:         vbUuid,
	}
}
