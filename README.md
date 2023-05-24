# Go Kafka Connect Couchbase

[![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-kafka-connect-couchbase.svg)](https://pkg.go.dev/github.com/Trendyol/go-kafka-connect-couchbase) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-kafka-connect-couchbase)](https://goreportcard.com/report/github.com/Trendyol/go-kafka-connect-couchbase)

Go implementation of the [Kafka Connect Couchbase](https://github.com/couchbase/kafka-connect-couchbase).

**Go Kafka Connect Couchbase** streams documents from Couchbase Database Change Protocol (DCP) and publishes Kafka
events in near real-time.

## Features

* **Less resource usage** and **higher throughput**(see [Benchmarks](#benchmarks)).
* **Custom Kafka key and headers** implementation(see [Example](#example)).
* Sending **multiple Kafka events for a DCP event**(see [Example](#example)).
* Easier to handle different DCP events such as **expiration, deletion and mutation**(see [Example](#example)).
* **Kafka compression** support(Gzip, Snappy, Lz4, Zstd).
* **Kafka producer acknowledges** support(fire-and-forget, wait for the leader, wait for the full ISR).
* Metadata can be saved to **Couchbase or Kafka**
* **Easier to manage batch configurations** such as maximum batch size, batch bytes, batch ticker durations.
* **Easier to scale up and down** by custom membership algorithms(Couchbase, KubernetesHa, Kubernetes StatefulSet or
  Static, see [examples](https://github.com/Trendyol/go-dcp-client#examples)).
* **Easier to configure**.

## Benchmarks

TODO
| Package | Time | Time % to | Objects Allocated |
| :------ | :--: | :-----------: | :---------------: |
| Java Kafka Connect Couchbase | 1744 ns/op | +0% | 5 allocs/op
| **Go Kafka Connect Couchbase** | 2483 ns/op | +42% | 10 allocs/op

## Example

[Basic](example/main.go)

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

## Configuration

### Dcp Configuration

Check out on [go-dcp-client](https://github.com/Trendyol/go-dcp-client#configuration)

### Kafka Specific Configuration

| Variable                            | Type              | Required | Default  | Description                                                                                                                                                                                                                                                                                      |                                                            
|-------------------------------------|-------------------|----------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kafka.collectionTopicMapping`      | map[string]string | yes      |          | Defines which Couchbase collection events will be sent to which topic                                                                                                                                                                                                                            | 
| `kafka.brokers`                     | []string          | yes      |          | Broker ip and port information                                                                                                                                                                                                                                                                   |
| `kafka.producerBatchSize`           | integer           | no       | 2000     | Maximum message count for batch, if exceed flush will be triggered.                                                                                                                                                                                                                              |
| `kafka.producerBatchBytes`          | integer           | no       | 10485760 | Maximum size(byte) for batch, if exceed flush will be triggered.                                                                                                                                                                                                                                 |
| `kafka.producerBatchTickerDuration` | time.Duration     | no       | 10s      | Batch is being flushed automatically at specific time intervals for long waiting messages in batch.                                                                                                                                                                                              |
| `kafka.readTimeout`                 | time.Duration     | no       | 30s      | segmentio/kafka-go - Timeout for read operations                                                                                                                                                                                                                                                 |
| `kafka.writeTimeout`                | time.Duration     | no       | 30s      | segmentio/kafka-go - Timeout for write operations                                                                                                                                                                                                                                                |
| `kafka.compression`                 | integer           | no       | 0        | Compression can be used if message size is large, CPU usage may be affected. 0=None, 1=Gzip, 2=Snappy, 3=Lz4, 4=Zstd                                                                                                                                                                             |
| `kafka.requiredAcks`                | integer           | no       | 1        | segmentio/kafka-go - Number of acknowledges from partition replicas required before receiving a response to a produce request. 0=fire-and-forget, do not wait for acknowledgements from the, 1=wait for the leader to acknowledge the writes, -1=wait for the full ISR to acknowledge the writes |
| `kafka.secureConnection`            | bool              | no       | false    | Enable secure Kafka.                                                                                                                                                                                                                                                                             |
| `kafka.rootCAPath`                  | string            | no       | *not set | Define root CA path.                                                                                                                                                                                                                                                                             |
| `kafka.interCAPath`                 | string            | no       | *not set | Define inter CA path.                                                                                                                                                                                                                                                                            |
| `kafka.scramUsername`               | string            | no       | *not set | Define scram username.                                                                                                                                                                                                                                                                           |
| `kafka.scramPassword`               | string            | no       | *not set | Define scram password.                                                                                                                                                                                                                                                                           |

## Maintainers

* [Eray Arslan](https://github.com/erayarslan)
* [Mehmet Sezer](https://github.com/mhmtszr)
* [Oğuzhan Yıldırım](https://github.com/oguzyildirim)

## Contributing

Go Kafka Connect Couchbase is always open for direct contributions. For more information please check
our [Contribution Guideline document](./CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).