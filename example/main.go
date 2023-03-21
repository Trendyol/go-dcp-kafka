package main

import (
	gokafkaconnectcouchbase "github.com/Trendyol/go-kafka-connect-couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/message"
)

func mapper(event couchbase.Event) *message.KafkaMessage {
	// return nil if you wish filter the event
	return message.GetKafkaMessage(event.Key, event.Value, nil)
}

func main() {
	connector, err := gokafkaconnectcouchbase.NewConnector("./example/config.yml", mapper)
	if err != nil {
		panic("New connector is could not initialized: " + err.Error())
	}
	defer connector.Close()

	connector.Start()
}
