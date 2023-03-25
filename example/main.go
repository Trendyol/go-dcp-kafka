package main

import (
	gokafkaconnectcouchbase "github.com/Trendyol/go-kafka-connect-couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/message"
)

func mapper(event couchbase.Event) []*message.KafkaMessage {
	// return empty if you wish filter the event
	return []*message.KafkaMessage{
		message.GetKafkaMessage(event.Key, event.Value, nil),
	}
}

func main() {
	connector, err := gokafkaconnectcouchbase.NewConnector("./example/config.yml", mapper)
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
