package integration

import (
	"context"
	dcpkafka "github.com/Trendyol/go-dcp-kafka"
	"github.com/Trendyol/go-dcp-kafka/config"
	"github.com/Trendyol/go-dcp-kafka/couchbase"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
	dcpConfig "github.com/Trendyol/go-dcp/config"
	"github.com/segmentio/kafka-go"
	"sync"
	"testing"
	"time"
)

func mapper(event couchbase.Event) []message.KafkaMessage {
	if event.IsExpired || event.IsDeleted {
		return nil
	}
	return []message.KafkaMessage{
		{
			Headers: nil,
			Key:     event.Key,
			Value:   event.Value,
		},
	}
}

func TestKafka(t *testing.T) {
	time.Sleep(time.Second * 30)

	connector, err := dcpkafka.NewConnectorBuilder(&config.Connector{
		Dcp: dcpConfig.Dcp{
			Hosts:      []string{"localhost:8091"},
			Username:   "user",
			Password:   "password",
			BucketName: "dcp-test",
			RollbackMitigation: dcpConfig.RollbackMitigation{
				Disabled: true,
			},
			Dcp: dcpConfig.ExternalDcp{
				Group: dcpConfig.DCPGroup{
					Name: "groupName",
					Membership: dcpConfig.DCPGroupMembership{
						Type: "static",
					},
				},
			},
			Metadata: dcpConfig.Metadata{
				ReadOnly: true,
				Config: map[string]string{
					"bucket":     "checkpoint-bucket-name",
					"scope":      "_default",
					"collection": "_default",
				},
				Type: "couchbase",
			},
			Debug: true},
		Kafka: config.Kafka{
			CollectionTopicMapping: map[string]string{"_default": "topicname"},
			Brokers:                []string{"localhost:9092"},
			ProducerBatchBytes:     104857600,
			ProducerBatchSize:      100,
			ReadTimeout:            30 * time.Second,
			WriteTimeout:           30 * time.Second,
			MetadataTTL:            2400 * time.Second,
			MetadataTopics:         []string{"topicname"},
		},
	}).SetMapper(mapper).Build()
	if err != nil {
		t.Fatal(err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		connector.Start()
	}()

	go func() {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:       []string{"localhost:9092"},
			Topic:         "topicname",
			QueueCapacity: 200,
			MaxBytes:      10e6, // 10MB
		})

		totalEvent := 0
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Minute)

		for {
			select {
			case <-ctx.Done():
				t.Fatalf("deadline exceed")
			default:
				m, err := r.ReadMessage(context.Background())
				if err != nil {
					t.Fatalf("error while reading topic %s", err)
				}
				if m.Value != nil {
					totalEvent += 1
				}
				if totalEvent == 31591 {
					connector.Close()
					break
				}
			}
		}
	}()

	wg.Wait()
}
