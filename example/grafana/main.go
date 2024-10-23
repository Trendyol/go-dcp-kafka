package main

import (
	"fmt"
	dcpkafka "github.com/Trendyol/go-dcp-kafka"
	"github.com/Trendyol/go-dcp-kafka/couchbase"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
	"github.com/couchbase/gocb/v2"
	"log"
	"time"
)

func mapper(event couchbase.Event) []message.KafkaMessage {
	log.Println("Captured: ", string(event.Key))
	return []message.KafkaMessage{
		{
			Headers: nil,
			Key:     event.Key,
			Value:   event.Value,
		},
	}
}

func main() {
	time.Sleep(15 * time.Second) //wait for couchbase container initialize

	go seedCouchbaseBucket()

	connector, err := dcpkafka.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		Build()

	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}

func seedCouchbaseBucket() {
	cluster, err := gocb.Connect("couchbase://couchbase", gocb.ClusterOptions{
		Username: "user",
		Password: "password",
	})
	if err != nil {
		log.Fatal(err)
	}

	bucket := cluster.Bucket("dcp-test")
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
	}

	collection := bucket.DefaultCollection()

	counter := 0
	for {
		counter++
		documentID := fmt.Sprintf("doc-%d", counter)
		document := map[string]interface{}{
			"counter": counter,
			"message": "Hello Couchbase",
			"time":    time.Now().Format(time.RFC3339),
		}
		_, err := collection.Upsert(documentID, document, nil)
		if err != nil {
			log.Println("Error inserting document:", err)
		} else {
			log.Println("Inserted document:", documentID)
		}
		time.Sleep(1 * time.Second)
	}
}
