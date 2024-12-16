package main

import (
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

func main() {
	c, err := dcpkafka.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		SetSinkResponseHandler(kafka.NewRejectionLogSinkResponseHandler()).
		Build()
	if err != nil {
		panic(err)
	}

	defer c.Close()
	c.Start()
}
