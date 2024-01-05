package kafka

import (
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
)

type SinkResponseHandlerContext struct {
	Message *message.KafkaMessage
	Err     error
}

type SinkResponseHandler interface {
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
}
