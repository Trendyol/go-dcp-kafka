package producer

import (
	"github.com/segmentio/kafka-go"
	"sync"
)

type Producer interface {
	Produce(message []byte, key []byte, headers map[string]string)
	Close() error
}

var MessagePool = sync.Pool{
	New: func() any {
		return &kafka.Message{}
	},
}

func NewHeaders(headersMap map[string]string) []kafka.Header {
	var headers []kafka.Header
	for key, value := range headersMap {
		headers = append(headers, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}
	return headers
}
