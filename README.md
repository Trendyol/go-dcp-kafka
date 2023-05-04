# Go Kafka Connect Couchbase [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-kafka-connect-couchbase.svg)](https://pkg.go.dev/github.com/Trendyol/go-kafka-connect-couchbase) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-kafka-connect-couchbase)](https://goreportcard.com/report/github.com/Trendyol/go-kafka-connect-couchbase)

This repository contains the Go implementation of the Couchbase Kafka Connector.

### Contents

---

* [What is Couchbase Kafka Connector?](#what-is-couchbase-kafka-connector)
* [Why?](#why)
* [Features](#features)
* [Usage](#usage)
* [Configuration](#configuration)
* [Examples](#examples)

### What is Couchbase Kafka Connector?

Official Couchbase documentation defines the Couchbase Kafka Connector as follows:

_The Couchbase Kafka connector is a plug-in for the Kafka Connect framework. It provides source and sink components._

The **source connector** streams documents from Couchbase Database Change Protocol (DCP) and publishes each document to
a Kafka topic in near real-time.

The **sink connector** subscribes to Kafka topics and writes the messages to Couchbase.

**Go Kafka Connect Couchbase is a source connector**. So it sends Couchbase mutations to Kafka as events.

---

### Why?

+ Build a Couchbase Kafka Connector by using **Go** to reduce resource usage.
+ Suggesting more **configurations** so users can make changes to less code.
+ By presenting the connector as a **library**, ensuring that users do not clone the code they don't need.

---

### Example

```go
package main

import (
	gokafkaconnectcouchbase "github.com/Trendyol/go-kafka-connect-couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/message"
)

func mapper(event couchbase.Event) []message.KafkaMessage {
	// return empty if you wish filter the event
	return []message.KafkaMessage{
		{
			Headers: nil,
			Key:     event.Key,
			Value:   event.Value,
		},
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
```

Custom log structures can be used with the connector

```go
package main

import (
	gokafkaconnectcouchbase "github.com/Trendyol/go-kafka-connect-couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/message"
	"log"
	"os"
)

func mapper(event couchbase.Event) []message.KafkaMessage {
	// return empty if you wish filter the event
	return []message.KafkaMessage{
		{
			Headers: nil,
			Key:     event.Key,
			Value:   event.Value,
		},
	}
}

func main() {
	logger := log.New(os.Stdout, "cb2kafka: ", log.Ldate|log.Ltime|log.Llongfile)

	connector, err := gokafkaconnectcouchbase.NewConnectorWithLoggers("./example/config.yml", mapper, logger, logger)
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
```

---

### Features

- [X] Batch Producer
- [X] Secure Kafka
- [X] Kafka Metadata

---

### Usage

```
$ go get github.com/Trendyol/go-kafka-connect-couchbase

```

---

### Dcp Configuration

Check out on [go-dcp-client](https://github.com/Trendyol/go-dcp-client#configuration)

### Kafka Specific Configuration

| Variable                            | Type              | Required | Default  | Description                             |                                                            
|-------------------------------------|-------------------|----------|----------|-----------------------------------------|
| `kafka.collectionTopicMapping`      | map[string]string | yes      |          |                                         | 
| `kafka.brokers`                     | []string          | yes      |          |                                         |
| `kafka.producerBatchSize`           | integer           | no       | 2000     |                                         |
| `kafka.producerBatchBytes`          | integer           | no       | 10485760 |                                         |
| `kafka.producerBatchTickerDuration` | time.Duration     | no       | 10s      |                                         |
| `kafka.readTimeout`                 | time.Duration     | no       | 30s      |                                         |
| `kafka.writeTimeout`                | time.Duration     | no       | 30s      |                                         |
| `kafka.compression`                 | integer           | no       | 0        | 0=None, 1=Gzip, 2=Snappy, 3=Lz4, 4=Zstd |
| `kafka.requiredAcks`                | integer           | no       | 1        |                                         |
| `kafka.secureConnection`            | bool              | no       | false    |                                         |
| `kafka.rootCAPath`                  | string            | no       | *not set |                                         |
| `kafka.interCAPath`                 | string            | no       | *not set |                                         |
| `kafka.scramUsername`               | string            | no       | *not set |                                         |
| `kafka.scramPassword`               | string            | no       | *not set |                                         |

---

### Examples

- [example](example/main.go)
