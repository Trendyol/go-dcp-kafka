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

func mapper(event couchbase.Event) *message.KafkaMessage {
	// return nil if you wish to filter the event
	return message.GetKafkaMessage(event.Key, event.Value, map[string]string{})
}

func main() {
	connector := gokafkaconnectcouchbase.NewConnector("./example/config.yml", mapper)

	defer connector.Close()

	connector.Start()
}

```

Custom log structures can be used with the connector

```go
package main

type ConnectorLogger struct{}

func (d *ConnectorLogger) Printf(msg string, args ...interface{}) {
	zapLogger.Info(fmt.Sprintf(msg, args...))
}

func main() {
	l := &ConnectorLogger{}
	connector := gokafkaconnectcouchbase.NewConnectorWithLoggers("./example/config.yml", mapper, l, l)

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

| Variable                            | Type              | Required | Default |                                                             
|-------------------------------------|-------------------|----------|---------|
| `kafka.collectionTopicMapping`      | map[string]string | yes      |         |
| `kafka.brokers`                     | []string          | yes      |         |
| `kafka.producerBatchSize`           | integer           | yes      |         |
| `kafka.producerBatchTickerDuration` | time.Duration     | yes      |         |
| `kafka.readTimeout`                 | time.Duration     | no       |         |
| `kafka.compression`                 | integer           | no       |         |
| `kafka.writeTimeout`                | time.Duration     | no       |         |
| `kafka.requiredAcks`                | integer           | no       |         |
| `kafka.secureConnection`            | bool              | no       |         |
| `kafka.rootCAPath`                  | string            | no       |         |
| `kafka.interCAPath`                 | string            | no       |         |
| `kafka.scramUsername`               | string            | no       |         |
| `kafka.scramPassword`               | string            | no       |         |

---

### Examples

- [example](example/main.go)
