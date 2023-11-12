package dcpkafka

import (
	"github.com/Trendyol/go-dcp-kafka/couchbase"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
)

type Mapper func(event couchbase.Event) []message.KafkaMessage

func DefaultMapper(event couchbase.Event) []message.KafkaMessage {
	if event.IsExpired || event.IsDeleted {
		return nil
	}
	return []message.KafkaMessage{
		{
			Key:   event.Key,
			Value: event.Value,
		},
	}
}
