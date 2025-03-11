package main

import (
	"github.com/Trendyol/go-dcp-kafka"
	"github.com/Trendyol/go-dcp-kafka/kafka/producer"
)

func main() {
	c, err := dcpkafka.NewConnectorBuilder("config.yml").
		SetMapper(dcpkafka.DefaultMapper).
		SetCompletionHandler(producer.DefaultCompletion).
		Build()
	if err != nil {
		panic(err)
	}

	defer c.Close()
	c.Start()
}
