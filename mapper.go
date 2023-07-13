package dcpkafka

import (
	"github.com/Trendyol/go-dcp-kafka/couchbase"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
)

type Mapper func(event couchbase.Event) []message.KafkaMessage
