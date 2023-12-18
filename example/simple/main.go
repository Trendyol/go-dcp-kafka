package main

import (
	"fmt"
	dcpkafka "github.com/Trendyol/go-dcp-kafka"
	"github.com/Trendyol/go-dcp-kafka/couchbase"
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

type callback struct{}

func (c *callback) OnSuccess(message message.KafkaMessage) {
	fmt.Printf("OnSuccess %v\n", string(message.Value))
}

func (c *callback) OnError(message message.KafkaMessage) {
	fmt.Printf("OnError %v\n", string(message.Value))
}

func main() {
	c, err := dcpkafka.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		SetCallback(&callback{}). // if you want to add callback func
		Build()
	if err != nil {
		panic(err)
	}

	defer c.Close()
	c.Start()
}
