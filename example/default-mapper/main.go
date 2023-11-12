package main

import (
	"github.com/Trendyol/go-dcp-kafka"
)

func main() {
	c, err := dcpkafka.NewConnectorBuilder("config.yml").SetMapper(dcpkafka.DefaultMapper).Build()
	if err != nil {
		panic(err)
	}

	defer c.Close()
	c.Start()
}
