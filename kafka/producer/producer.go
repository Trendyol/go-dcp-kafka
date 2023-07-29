package producer

import (
	"time"

	"github.com/Trendyol/go-dcp-kafka/config"
	gKafka "github.com/Trendyol/go-dcp-kafka/kafka"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
	"github.com/segmentio/kafka-go"
)

type Metric struct {
	KafkaConnectorLatency int64
	BatchProduceLatency   int64
}

type Producer struct {
	ProducerBatch *Batch
}

func NewProducer(kafkaClient gKafka.Client,
	config *config.Connector,
	logger logger.Logger,
	errorLogger logger.Logger,
	dcpCheckpointCommit func(),
) (Producer, error) {
	writer := kafkaClient.Producer()

	return Producer{
		ProducerBatch: newBatch(
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

func (p *Producer) StartBatch() {
	p.ProducerBatch.StartBatchTicker()
}

func (p *Producer) Produce(
	ctx *models.ListenerContext,
	eventTime time.Time,
	messages []kafka.Message,
) {
	p.ProducerBatch.AddMessages(ctx, messages, eventTime)
}

func (p *Producer) Close() error {
	p.ProducerBatch.Close()
	return p.ProducerBatch.Writer.Close()
}

func (p *Producer) GetMetric() *Metric {
	return p.ProducerBatch.metric
}
