package kafka

import (
	"github.com/Trendyol/go-dcp-kafka/config"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
	"github.com/segmentio/kafka-go"
)

type SinkResponseHandlerContext struct {
	Message *message.KafkaMessage
	Err     error
}

type SinkResponseHandlerInitContext struct {
	KafkaClient Client
	Writer      *kafka.Writer
	Config      config.Kafka
}

type SinkResponseHandler interface {
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
	OnInit(ctx *SinkResponseHandlerInitContext)
}
