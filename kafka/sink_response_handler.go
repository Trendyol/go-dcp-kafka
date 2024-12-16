package kafka

import (
	"github.com/Trendyol/go-dcp-kafka/config"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
)

type SinkResponseHandlerContext struct {
	Message *message.KafkaMessage
	Err     error
}

type SinkResponseHandlerInitContext struct {
	SinkResponseHandlerContext
	KafkaClient Client
	Config      config.Kafka
}

type SinkResponseHandler interface {
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
	OnInit(ctx *SinkResponseHandlerInitContext)
}
