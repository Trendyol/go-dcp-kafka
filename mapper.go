package main

import (
	"connector/couchbase"
	"connector/kafka"
)

type Mapper func(event *couchbase.Event) *kafka.Message
