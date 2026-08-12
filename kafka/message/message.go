package message

import (
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaMessage struct {
	EventTime time.Time
	Topic     string
	Headers   []kafka.Header
	Key       []byte
	Value     []byte
}
