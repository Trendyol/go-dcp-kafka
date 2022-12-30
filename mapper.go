package gokafkaconnectcouchbase

import (
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka"
)

type Mapper func(event *couchbase.Event) *kafka.Message
