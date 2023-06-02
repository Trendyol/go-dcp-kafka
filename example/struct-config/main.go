package main

import (
	dcpClientConfig "github.com/Trendyol/go-dcp-client/config"
	dcpkafka "github.com/Trendyol/go-kafka-connect-couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/message"
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
		Dcp: dcpClientConfig.Dcp{
			Hosts:      []string{"localhost:8091"},
			Username:   "user",
			Password:   "password",
			BucketName: "dcp-test",
			Dcp: dcpClientConfig.ExternalDcp{
				Group: dcpClientConfig.DCPGroup{
					Name: "groupName",
					Membership: dcpClientConfig.DCPGroupMembership{
						RebalanceDelay: 3 * time.Second,
					},
				},
			},
			Metadata: dcpClientConfig.Metadata{
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
