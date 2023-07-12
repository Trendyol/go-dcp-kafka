package main

import (
	"github.com/Trendyol/go-dcp-kafka"
	"github.com/Trendyol/go-dcp-kafka/config"
	"github.com/Trendyol/go-dcp-kafka/couchbase"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
	dcpConfig "github.com/Trendyol/go-dcp/config"
	"time"
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
	c, err := dcpkafka.NewConnector(&config.Connector{
		Dcp: dcpConfig.Dcp{
			Hosts:      []string{"localhost:8091"},
			Username:   "user",
			Password:   "password",
			BucketName: "dcp-test",
			Dcp: dcpConfig.ExternalDcp{
				Group: dcpConfig.DCPGroup{
					Name: "groupName",
					Membership: dcpConfig.DCPGroupMembership{
						RebalanceDelay: 3 * time.Second,
					},
				},
			},
			Metadata: dcpConfig.Metadata{
				Config: map[string]string{
					"bucket":     "checkpoint-bucket-name",
					"scope":      "_default",
					"collection": "_default",
				},
				Type: "couchbase",
			},
			Debug: true},
		Kafka: config.Kafka{
			CollectionTopicMapping: map[string]string{"_default": "topic"},
			Brokers:                []string{"localhost:9092"},
		},
	}, mapper)
	if err != nil {
		panic(err)
	}

	defer c.Close()
	c.Start()
}
