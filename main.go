package godcpkafkaconnector

import (
	"godcpkafkaconnector/couchbase"
	"godcpkafkaconnector/kafka"
)

func mapper(event *couchbase.Event) *kafka.Message {
	return &kafka.Message{
		Key:     &event.Key,
		Value:   event.Value,
		Headers: map[string]string{},
	}
}

func main() {
	connector := NewConnector("./config.yml", mapper)

	defer connector.Close()

	connector.Start()
}
