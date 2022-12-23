package godcpkafkaconnector

import (
	"godcpkafkaconnector/couchbase"
	"godcpkafkaconnector/kafka"
)

type Mapper func(event *couchbase.Event) *kafka.Message
