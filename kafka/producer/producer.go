package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"godcpkafkaconnector/config"
	"godcpkafkaconnector/logger"
	"strings"
)

type Producer interface {
	Produce(ctx *context.Context, message []byte, key []byte, headers map[string]string) error
	Close() error
}

type producer struct {
	writer *kafka.Writer
}

func NewProducer(config *config.Kafka, logger logger.Logger, errorLogger logger.Logger) Producer {
	writer := &kafka.Writer{
		Topic:        config.Topic,
		Addr:         kafka.TCP(strings.Split(config.Brokers, ",")...),
		Balancer:     &kafka.Hash{},
		MaxAttempts:  config.MaxAttempts,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		RequiredAcks: kafka.RequireOne,
		Logger:       logger,
		ErrorLogger:  errorLogger,
	}
	return &producer{
		writer: writer,
	}
}

func (a *producer) Produce(ctx *context.Context, message []byte, key []byte, headers map[string]string) error {
	return a.writer.WriteMessages(*ctx, kafka.Message{Value: message, Headers: newHeaders(headers), Key: key})
}

func newHeaders(headersMap map[string]string) []kafka.Header {
	var headers []kafka.Header
	for key, value := range headersMap {
		headers = append(headers, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}
	return headers
}

func (a *producer) Close() error {
	return a.writer.Close()
}
