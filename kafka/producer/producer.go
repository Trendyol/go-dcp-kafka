package kafka

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/logger"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(ctx *context.Context, message []byte, key []byte, headers map[string]string) error
	Close() error
}

type producer struct {
	producerBatch *producerBatch
}

func NewProducer(config *config.Kafka, logger logger.Logger, errorLogger logger.Logger) Producer {
	writer := &kafka.Writer{
		Topic:        config.Topic,
		Addr:         kafka.TCP(strings.Split(config.Brokers, ",")...),
		Balancer:     &kafka.Hash{},
		MaxAttempts:  math.MaxInt,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(config.RequiredAcks),
		Logger:       logger,
		ErrorLogger:  errorLogger,
	}
	return &producer{
		producerBatch: newProducerBatch(config.ProducerBatchTickerDuration, writer, config.ProducerBatchSize, logger, errorLogger),
	}
}

func (a *producer) Produce(ctx *context.Context, message []byte, key []byte, headers map[string]string) error {
	kafkaMessage := kafkaMessagePool.Get().(kafka.Message)

	kafkaMessage.Key = key
	kafkaMessage.Value = message
	kafkaMessage.Headers = newHeaders(headers)

	return a.producerBatch.AddMessage(kafkaMessage)
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
	a.producerBatch.isClosed <- true
	// TODO: Wait until batch is clear
	time.Sleep(2 * time.Second)
	return a.producerBatch.Writer.Close()
}
