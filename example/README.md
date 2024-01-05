# How to Create a New Connector

If you want to create a new connector that streams mutations from Couchbase to Kafka, you can use the `go-dcp-kafka` library. 
This library provides a simple and flexible way to implement connectors that can be customized to your needs.

## Step 1: Installing the Library

To use the `go-dcp-kafka` library, you first need to install it. You can do this using the `go get` command:

```
$ go get github.com/Trendyol/go-dcp-kafka

```

## Step 2: Implementing the Mapper

The mapper is a function that takes a `couchbase.Event` as input and returns a slice of `message.KafkaMessage`. 
The mapper is responsible for converting the Couchbase mutations to Kafka events that will be sent to Kafka.

Here's an example mapper implementation:

```go
package main

import (
	"github.com/Trendyol/go-dcp-kafka/couchbase"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
)

func mapper(event couchbase.Event) []message.KafkaMessage {
	// here you can filter and transform events
	return []message.KafkaMessage{
		{
			Headers: nil,
			Key:     event.Key,
			Value:   event.Value,
		},
	}
}
```

## Step 3: Implementing the SinkResponseHandler

This function is called after the event is published and takes `message.KafkaMessage` as a parameter.
Here's an example SinkResponseHandler implementation:

```go
type sinkResponseHandler struct {
}

func (s *sinkResponseHandler) OnSuccess(ctx *kafka.SinkResponseHandlerContext) {
fmt.Printf("OnSuccess %v\n", string(ctx.Message.Value))
}

func (s *sinkResponseHandler) OnError(ctx *kafka.SinkResponseHandlerContext) {
fmt.Printf("OnError %v\n", string(ctx.Message.Value))
}
```

## Step 4: Configuring the Connector

The configuration for the connector is provided via a YAML file. Here's an example [configuration](https://github.com/Trendyol/go-dcp-kafka/blob/master/example/config.yml):

You can find explanation of [configurations](https://github.com/Trendyol/go-dcp#configuration)

You can pass this configuration file to the connector by providing the path to the file when creating the connector:
```go
connector, err := dcpkafka.NewConnectorBuilder("config.yml").
    SetMapper(mapper).
	SetSinkResponseHandler(&sinkResponseHandler{}). // if you want to add callback func
    Build()

if err != nil {
	panic(err)
}
```

## Step 5: Starting and Closing the Connector

Once you have implemented the mapper and configured the connector, you can start/stop the connector:

```go
defer connector.Close()
connector.Start()
```

This will start the connector, which will continuously listen for Couchbase mutations and stream events to Kafka according to the configured mapper.

## Monitoring

The connector features an API that allows for management and observation, and it also exposes multiple metrics to facilitate these tasks.

You can find explanation [here](https://github.com/Trendyol/go-dcp#monitoring)