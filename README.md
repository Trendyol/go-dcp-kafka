# Go Dcp Kafka [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp-kafka.svg)](https://pkg.go.dev/github.com/Trendyol/go-dcp-kafka) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-dcp-kafka)](https://goreportcard.com/report/github.com/Trendyol/go-dcp-kafka) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Trendyol/go-dcp-kafka/badge)](https://scorecard.dev/viewer/?uri=github.com/Trendyol/go-dcp-kafka)

Go implementation of the [Kafka Connect Couchbase](https://github.com/couchbase/kafka-connect-couchbase).

**Go Dcp Kafka** streams documents from Couchbase Database Change Protocol (DCP) and publishes Kafka
events in near real-time.

## Features

* **Less resource usage** and **higher throughput**(see [Benchmarks](#benchmarks)).
* **Custom Kafka key and headers** implementation(see [Example](#example)).
* Sending **multiple Kafka events for a DCP event**(see [Example](#example)).
* Handling different DCP events such as **expiration, deletion and mutation**(see [Example](#example)).
* **Kafka compression** support(Gzip, Snappy, Lz4, Zstd).
* **Kafka producer acknowledges** support(fire-and-forget, wait for the leader, wait for the full ISR).
* Metadata can be saved to **Couchbase or Kafka**.
* **Managing batch configurations** such as maximum batch size, batch bytes, batch ticker durations.
* **Scale up and down** by custom membership algorithms(Couchbase, KubernetesHa, Kubernetes StatefulSet or
  Static, see [examples](https://github.com/Trendyol/go-dcp#examples)).
* **Easily manageable configurations**.

## Benchmarks

The benchmark was made with the  **1,001,006** Couchbase document, because it is possible to more clearly observe the
difference in the batch structure between the two packages. **Default configurations** for Java Kafka Connect Couchbase
used for both connectors.

| Package                             | Time to Process Events | Average CPU Usage(Core) | Average Memory Usage |
|:------------------------------------|:----------------------:|:-----------------------:|:--------------------:|
| **Go Dcp Kafka**(1.20)              |        **12s**         |        **0.383**        |      **428MB**       
| Java Kafka Connect Couchbase(JDK11) |          19s           |           1.5           |        932MB         

## Example

[Struct Config](example/struct-config/main.go)

```go
func mapper(event couchbase.Event) []message.KafkaMessage {
	// return nil if you wish to discard the event
	return []message.KafkaMessage{
		{
			Headers: nil,
			Key:     event.Key,
			Value:   event.Value,
			Writer:  event.Value
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
```

[File Config](example/simple/main.go)

[File Config](example/default-mapper/main.go)

## Configuration

### Dcp Configuration

Check out on [go-dcp](https://github.com/Trendyol/go-dcp#configuration)

### Kafka Specific Configuration

| Variable                            | Type              | Required | Default     | Description                                                                                                                                                                                                                                                                                      |                                                            
|-------------------------------------|-------------------|----------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kafka.collectionTopicMapping`      | map[string]string | yes      |             | Defines which Couchbase collection events will be sent to which topic,:warning: **If topic information is entered in the mapper, it will OVERWRITE this config**.                                                                                                                                | 
| `kafka.brokers`                     | []string          | yes      |             | Broker ip and port information                                                                                                                                                                                                                                                                   |
| `kafka.producerBatchSize`           | integer           | no       | 2000        | Maximum message count for batch, if exceed flush will be triggered.                                                                                                                                                                                                                              |
| `kafka.producerBatchBytes`          | 64 bit integer    | no       | 1mb         | Maximum size(byte) for batch, if exceed flush will be triggered. `1mb` is default.                                                                                                                                                                                                               |
| `kafka.producerMaxAttempts`         | int               | no       | math.MaxInt | Limit on how many attempts will be made to deliver a message.                                                                                                                                                                                                                                    |
| `kafka.producerBatchTickerDuration` | time.Duration     | no       | 10s         | Batch is being flushed automatically at specific time intervals for long waiting messages in batch.                                                                                                                                                                                              |
| `kafka.readTimeout`                 | time.Duration     | no       | 30s         | segmentio/kafka-go - Timeout for read operations                                                                                                                                                                                                                                                 |
| `kafka.writeTimeout`                | time.Duration     | no       | 30s         | segmentio/kafka-go - Timeout for write operations                                                                                                                                                                                                                                                |
| `kafka.compression`                 | integer           | no       | 0           | Compression can be used if message size is large, CPU usage may be affected. 0=None, 1=Gzip, 2=Snappy, 3=Lz4, 4=Zstd                                                                                                                                                                             |
| `kafka.balancer`                    | string            | no       | Hash        | Define balancer strategy. Available fields: Hash, LeastBytes, RoundRobin, ReferenceHash, CRC32Balancer, Murmur2Balancer.                                                                                                                                                                         |
| `kafka.requiredAcks`                | integer           | no       | 1           | segmentio/kafka-go - Number of acknowledges from partition replicas required before receiving a response to a produce request. 0=fire-and-forget, do not wait for acknowledgements from the, 1=wait for the leader to acknowledge the writes, -1=wait for the full ISR to acknowledge the writes |
| `kafka.secureConnection`            | bool              | no       | false       | Enable secure Kafka.                                                                                                                                                                                                                                                                             |
| `kafka.rootCAPath`                  | string            | no       | *not set    | Define root CA path.                                                                                                                                                                                                                                                                             |
| `kafka.interCAPath`                 | string            | no       | *not set    | Define inter CA path.                                                                                                                                                                                                                                                                            |
| `kafka.scramUsername`               | string            | no       | *not set    | Define scram username.                                                                                                                                                                                                                                                                           |
| `kafka.scramPassword`               | string            | no       | *not set    | Define scram password.                                                                                                                                                                                                                                                                           |
| `kafka.metadataTTL`                 | time.Duration     | no       | 60s         | TTL for the metadata cached by segmentio, increase it to reduce network requests. For more detail please check [docs](https://pkg.go.dev/github.com/segmentio/kafka-go#Transport.MetadataTTL).                                                                                                   |
| `kafka.metadataTopics`              | []string          | no       |             | Topic names for the metadata cached by segmentio, define topics here that the connector may produce. In large Kafka clusters, this will reduce memory usage. For more detail please check [docs](https://pkg.go.dev/github.com/segmentio/kafka-go#Transport.MetadataTopics).                     |
| `kafka.clientID`                    | string            | no       |             | Unique identifier that the transport communicates to the brokers when it sends requests. For more detail please check [docs](https://pkg.go.dev/github.com/segmentio/kafka-go#Transport.ClientID).                                                                                               |
| `kafka.allowAutoTopicCreation`      | bool              | no       | false       | Create topic if missing. For more detail please check [docs](https://pkg.go.dev/github.com/segmentio/kafka-go#Writer.AllowAutoTopicCreation).                                                                                                                                                    |
| `kafka.rejectionLog.topic`          | string            | no       |             | Rejection topic name.                                                                                                                                                                                                                                                                            |
| `kafka.rejectionLog.includeValue`   | boolean           | no       | false       | Includes rejection log source info. `false` is default.                                                                                                                                                                                                                                          |

### Kafka Metadata Configuration(Use it if you want to store the checkpoint data in Kafka)

| Variable            | Type              | Description                                                                        |                                                            
|---------------------|-------------------|------------------------------------------------------------------------------------|
| `metadata.type`     | string            | Metadata storing types.  `kafka`,`file` or `couchbase`.                            |
| `metadata.readOnly` | bool              | Set this for debugging state purposes.                                             |
| `metadata.config`   | map[string]string | Set key-values of config. `topic`,`partition`,`replicationFactor` for `kafka` type |

## Exposed metrics

| Metric Name                              		| Description                            | Labels | Value Type |
|-------------------------------------------------------|----------------------------------------|--------|------------|
| cbgo_kafka_connector_latency_ms_current  		| Time to adding to the batch.           | N/A    | Gauge      |
| cbgo_kafka_connector_batch_produce_latency_ms_current | Time to produce messages in the batch. | N/A    | Gauge      |

You can also use all DCP-related metrics explained [here](https://github.com/Trendyol/go-dcp#exposed-metrics).
All DCP-related metrics are automatically injected. It means you don't need to do anything. 

## Breaking Changes

| Date taking effect | Date announced | Change | How to check    |
|--------------------| ---- |---- |-----------------| 
| November 11, 2023  | November 11, 2023 |  Creating connector via builder | Compile project |

## Grafana Metric Dashboard

[Grafana & Prometheus Example](example/grafana)

## Contributing

Go Dcp Kafka is always open for direct contributions. For more information please check
our [Contribution Guideline document](./CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).
