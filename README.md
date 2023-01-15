# Go Kafka Connect Couchbase

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

The **source connector** streams documents from Couchbase Database Change Protocol (DCP) and publishes each document to a Kafka topic in near real-time.

The **sink connector** subscribes to Kafka topics and writes the messages to Couchbase.

**Go Kafka Connect Couchbase is a source connector**. So it sends Couchbase mutations to Kafka as events.

---

### Why?

+ Build a Couchbase Kafka Connector by using **Go** to reduce resource usage.
+ Suggesting more **configurations** so users can make changes to less code.
+ By presenting the connector as a **library**, ensuring that users do not clone the code they don't need.

---

### Example
```
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
```
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

---

### Usage

```
$ go get github.com/Trendyol/go-kafka-connect-couchbase

```

---

### Configuration

| Variable                            | Type                 | Is Required |
|-------------------------------------|----------------------|-------------|
| `hosts`                             | array                | yes         |
| `username`                          | string               | yes         |
| `password`                          | string               | yes         |
| `bucketName`                        | string               | yes         |
| `scopeName`                         | string               | no          |
| `collectionNames`                   | array                | no          |
| `metadataBucket`                    | string               | no          |
| `dcp.group.name`                    | string               | yes         |
| `dcp.group.membership.type`         | string               | yes         |
| `dcp.group.membership.memberNumber` | integer              | no          |
| `dcp.group.membership.totalMembers` | integer              | no          |
| `kafka.collectionTopicMapping`      | map[string][string]  | yes         |
| `kafka.brokers`                     | array                | yes         |
| `kafka.readTimeout`                 | integer              | no          |
| `kafka.writeTimeout`                | integer              | no          |
| `kafka.producerBatchSize`           | integer              | yes         |
| `kafka.producerBatchTickerDuration` | integer              | yes         |
| `kafka.requiredAcks`                | integer              | no          |
| `kafka.secureConnection`            | boolean (true/false) | no          |
| `kafka.rootCAPath`                  | string               | no          |
| `kafka.interCAPath`                 | string               | no          |
| `kafka.scramUsername`               | string               | no          |
| `kafka.scramPassword`               | string               | no          |
| `logger.level`                      | string               | no          |
| `checkpoint.timeout`                | integer              | no          |

---

### Examples

- [example](example/main.go)
