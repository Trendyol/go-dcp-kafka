package message

import (
	"sync"
)

type KafkaMessage struct {
	Headers map[string]string
	Key     []byte
	Value   []byte
}

func GetKafkaMessage(key []byte, value []byte, headers map[string]string) *KafkaMessage {
	message := KafkaMessagePool.Get().(*KafkaMessage)
	message.Key = key
	message.Value = value
	message.Headers = headers
	return message
}

var KafkaMessagePool = sync.Pool{
	New: func() any {
		return &KafkaMessage{}
	},
}
