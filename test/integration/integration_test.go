package integration

import (
	"context"
	dcpkafka "github.com/Trendyol/go-dcp-kafka"
	"github.com/Trendyol/go-dcp-kafka/couchbase"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
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
	// Create topic
	_, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "test", 0)
	if err != nil {
		t.Fatalf("error while creating topic %s", err)
	}

	connector, err := dcpkafka.NewConnector("config.yml", mapper)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		connector.Start()
	}()

	go func() {
		time.Sleep(20 * time.Second)

		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:       []string{"localhost:9092"},
			Topic:         "test",
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
	t.Log("done")
}

type CountResponse struct {
	Count int64 `json:"count"`
}
