package message

import "github.com/segmentio/kafka-go"

type KafkaMessage struct {
	Headers []kafka.Header
	Key     []byte
	Value   []byte
	Topic   *string
}
