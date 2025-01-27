package main

import (
	"fmt"
	dcpkafka "github.com/Trendyol/go-dcp-kafka"
	"github.com/Trendyol/go-dcp-kafka/couchbase"
	"github.com/Trendyol/go-dcp-kafka/kafka"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
)

func mapper(event couchbase.Event) []message.KafkaMessage {
	// return nil if you wish to discard the event
	return []message.KafkaMessage{
		{
			Headers: nil,
			Key:     event.Key,
			Value:   event.Value,
		},
	}
}

type sinkResponseHandler struct {
}

func (s *sinkResponseHandler) OnInit(ctx *kafka.SinkResponseHandlerInitContext) {}

func (s *sinkResponseHandler) OnSuccess(ctx *kafka.SinkResponseHandlerContext) {
	fmt.Printf("OnSuccess Key: %v, Len: %v\n", string(ctx.Message.Key), len(ctx.Message.Value))
}

func (s *sinkResponseHandler) OnError(ctx *kafka.SinkResponseHandlerContext) {
	fmt.Printf("OnError Key: %v, Len: %v, Err: %v\n", string(ctx.Message.Key), len(ctx.Message.Value), ctx.Err)
}

func main() {
	c, err := dcpkafka.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		SetSinkResponseHandler(&sinkResponseHandler{}).
		Build()
	if err != nil {
		panic(err)
	}

	defer c.Close()
	c.Start()
}
