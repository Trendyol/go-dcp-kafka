package main

import (
	gokafkaconnectcouchbase "github.com/Trendyol/go-kafka-connect-couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka"
)

func mapper(event *couchbase.Event) *kafka.Message {
	// return nil if you wish filter the event
	return &kafka.Message{
		Key:     event.Key,
		Value:   event.Value,
		Headers: nil,
	}
}

func main() {
	connector := gokafkaconnectcouchbase.NewConnector("./example/config.yml", mapper)

	defer connector.Close()

	connector.Start()
}
