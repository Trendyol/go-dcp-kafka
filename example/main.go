package main

import (
	"godcpkafkaconnector"
	"godcpkafkaconnector/couchbase"
	"godcpkafkaconnector/kafka"
)

func mapper(event *couchbase.Event) *kafka.Message {
	return &kafka.Message{
		Key:     event.Key,
		Value:   event.Value,
		Headers: map[string]string{},
	}
}

func main() {
	connector := godcpkafkaconnector.NewConnector("./example/config.yml", mapper)

	defer connector.Close()

	connector.Start()
}
