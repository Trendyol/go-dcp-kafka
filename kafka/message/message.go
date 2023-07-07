package message

import "github.com/segmentio/kafka-go"

type KafkaMessage struct {
	Topic   string
	Headers []kafka.Header
	Key     []byte
	Value   []byte
}
