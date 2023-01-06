package kafka

import (
	"math"
	"sync"
	"time"

	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/logger"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(message []byte, key []byte, headers map[string]string)
	Close() error
}

type producer struct {
	producerBatch *producerBatch
}

func NewProducer(config *config.Kafka, logger logger.Logger, errorLogger logger.Logger) Producer {
	writer := &kafka.Writer{
		Topic:        config.Topic,
		Addr:         kafka.TCP(config.Brokers...),
		Balancer:     &kafka.Hash{},
		BatchSize:    config.ProducerBatchSize,
		BatchBytes:   math.MaxInt,
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

var KafkaMessagePool = sync.Pool{
	New: func() any {
		return &kafka.Message{}
	},
}

func (a *producer) Produce(message []byte, key []byte, headers map[string]string) {
	msg := KafkaMessagePool.Get().(*kafka.Message)
	msg.Key = key
	msg.Value = message
	msg.Headers = newHeaders(headers)
	a.producerBatch.messageChn <- msg
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
