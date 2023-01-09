# Go Kafka Connect Couchbase

This repository contains go implementation of a Couchbase Kafka Connector.

### Contents
---

* [Why?](#why)
* [Features](#features)
* [Usage](#usage)
* [Configuration](#configuration)
* [Examples](#examples)

### Why?

+ Our main goal is to build a kafka-connector for faster and stateful systems. 

---

### Example
```
package main

func mapper(event *couchbase.Event) *kafka.Message {
	// return nil if you wish filter the event
	return &kafka.Message{
		Key:     event.Key,
		Value:   event.Value,
		Headers: map[string]string{},
	}
}

func main() {
	connector := godcpkafkaconnector.NewConnector("./example/config.yml", mapper)

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

| Variable                            | Type                        | Is Required |
|-------------------------------------|-----------------------------|-------------|
| `hosts`                             | array                       | yes         |
| `username`                          | string                      | yes         |
| `password`                          | string                      | yes         |
| `bucketName`                        | string                      | yes         |
| `scopeName`                         | string                      | no          |
| `collectionNames`                   | array                       | no          |
| `metadataBucket`                    | string                      | no          |
| `dcp.group.name`                    | string                      | yes         |
| `dcp.group.membership.type`         | string                      | yes         |
| `dcp.group.membership.memberNumber` | integer                     | no          |
| `dcp.group.membership.totalMembers` | integer                     | no          |
| `kafka.topic`                       | string                      | yes         |
| `kafka.brokers`                     | array                       | yes         |
| `kafka.readTimeout`                 | integer                     | no          |
| `kafka.writeTimeout`                | integer                     | no          |
| `kafka.producerBatchSize`           | integer                     | no          |
| `kafka.producerBatchTickerDuration` | integer                     | no          |
| `kafka.requiredAcks`                | integer                     | no          |
| `kafka.secureConnection`            | boolean (true/false)        | no          |
| `kafka.rootCAPath`                  | string                      | no          |
| `kafka.interCAPath`                 | string                      | no          |
| `kafka.scramUsername`               | string                      | no          |
| `kafka.scramPassword`               | string                      | no          |
| `logger.level`                      | string                      | no          |
| `checkpoint.timeout`                | integer                     | no          |

---

### Examples

- [example](example/main.go)
