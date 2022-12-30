go-kafka-couchbase-connector
============================

### Usage

```
$ go get github.com/Trendyol/go-kafka-connect-couchbase
```
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

### Examples

- [example](example/main.go)
- [static membership config](example/config.yml)
- [kubernetesStatefulSet membership config](example/config_k8s_stateful_set.yml)
- [kubernetesHa membership config](example/config_k8s_leader_election.yml)
