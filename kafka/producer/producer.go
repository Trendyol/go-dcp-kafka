package producer

import (
	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	gKafka "github.com/Trendyol/go-kafka-connect-couchbase/kafka"
	"github.com/Trendyol/go-kafka-connect-couchbase/logger"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(ctx *models.ListenerContext, message []byte, key []byte, headers map[string]string, topic string)
	Close() error
}

type producer struct {
	producerBatch *producerBatch
}

func NewProducer(kafkaClient gKafka.Client, config *config.Kafka, logger logger.Logger, errorLogger logger.Logger, dcpCheckpointCommit func()) (Producer, error) {
	writer := kafkaClient.Producer()

	return &producer{
		producerBatch: newProducerBatch(
			config.ProducerBatchTickerDuration,
			writer,
			config.ProducerBatchSize,
			logger,
			errorLogger,
			dcpCheckpointCommit),
	}, nil
}

func (a *producer) Produce(ctx *models.ListenerContext, message []byte, key []byte, headers map[string]string, topic string) {
	a.producerBatch.AddMessage(ctx, message, key, newHeaders(headers), topic)
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
	a.producerBatch.Close()
	return a.producerBatch.Writer.Close()
}
