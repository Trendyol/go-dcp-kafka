package producer

import (
	"time"

	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/models"
	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	gKafka "github.com/Trendyol/go-kafka-connect-couchbase/kafka"
	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(ctx *models.ListenerContext, eventTime time.Time, message []byte, key []byte, headers map[string]string, topic string)
	Close() error
	GetMetric() *Metric
}

type Metric struct {
	KafkaConnectorLatency int64
}

type producer struct {
	producerBatch *producerBatch
}

func NewProducer(kafkaClient gKafka.Client,
	config *config.Config,
	logger logger.Logger,
	errorLogger logger.Logger,
	dcpCheckpointCommit func(),
) (Producer, error) {
	writer := kafkaClient.Producer()

	return &producer{
		producerBatch: newProducerBatch(
			config.Kafka.ProducerBatchTickerDuration,
			writer,
			config.Kafka.ProducerBatchSize,
			config.Kafka.ProducerBatchBytes,
			logger,
			errorLogger,
			dcpCheckpointCommit,
		),
	}, nil
}

func (p *producer) Produce(
	ctx *models.ListenerContext,
	eventTime time.Time,
	message []byte,
	key []byte,
	headers map[string]string,
	topic string,
) {
	p.producerBatch.AddMessage(ctx, kafka.Message{Key: key, Value: message, Headers: newHeaders(headers), Topic: topic}, eventTime)
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

func (p *producer) Close() error {
	p.producerBatch.Close()
	return p.producerBatch.Writer.Close()
}

func (p *producer) GetMetric() *Metric {
	return p.producerBatch.metric
}
