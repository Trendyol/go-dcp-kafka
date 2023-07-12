package producer

import (
	"time"

	"github.com/Trendyol/go-dcp-kafka/config"
	gKafka "github.com/Trendyol/go-dcp-kafka/kafka"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(ctx *models.ListenerContext, eventTime time.Time, messages []kafka.Message)
	Close() error
	GetMetric() *Metric
	StartBatch()
}

type Metric struct {
	KafkaConnectorLatency int64
	BatchProduceLatency   int64
}

type producer struct {
	producerBatch *producerBatch
}

func NewProducer(kafkaClient gKafka.Client,
	config *config.Connector,
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

func (p *producer) StartBatch() {
	p.producerBatch.StartBatchTicker()
}

func (p *producer) Produce(
	ctx *models.ListenerContext,
	eventTime time.Time,
	messages []kafka.Message,
) {
	p.producerBatch.AddMessages(ctx, messages, eventTime)
}

func (p *producer) Close() error {
	p.producerBatch.Close()
	return p.producerBatch.Writer.Close()
}

func (p *producer) GetMetric() *Metric {
	return p.producerBatch.metric
}
