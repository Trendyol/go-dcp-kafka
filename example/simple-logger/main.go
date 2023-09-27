package main

import (
	"github.com/Trendyol/go-dcp-kafka"
	"github.com/Trendyol/go-dcp-kafka/couchbase"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
	"github.com/sirupsen/logrus"
)

func mapper(event couchbase.Event) []message.KafkaMessage {
	// return nil if you wish to discard the event
	return []message.KafkaMessage{
		{
			Headers: nil,
			Key:     event.Key,
			Value:   event.Value,
		},
	}
}

func main() {
	logger := createLogger()

	c, err := dcpkafka.NewConnectorWithLoggers("config.yml", mapper, logger)
	if err != nil {
		panic(err)
	}

	defer c.Close()
	c.Start()
}

func createLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	formatter := &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyMsg: "ms",
		},
	}

	logger.SetFormatter(formatter)
	return logger
}
