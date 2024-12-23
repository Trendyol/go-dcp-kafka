package message

import "github.com/segmentio/kafka-go"

type KafkaMessage struct {
	Topic   string
	Headers []kafka.Header
	Key     []byte
	Value   []byte
	SeqNo   uint64
	VbID    uint16
}

func (m *KafkaMessage) IsHeaderExist(key string) bool {
	for _, header := range m.Headers {
		if header.Key == key {
			return true
		}
	}

	return false
}
