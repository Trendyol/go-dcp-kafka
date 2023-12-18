package producer

import (
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
	"time"

	"github.com/Trendyol/go-dcp/helpers"

	"github.com/Trendyol/go-dcp-kafka/config"
	gKafka "github.com/Trendyol/go-dcp-kafka/kafka"
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

type Callback interface {
	OnSuccess(message message.KafkaMessage)
	OnError(message message.KafkaMessage)
}

func NewProducer(kafkaClient gKafka.Client,
	config *config.Connector,
	dcpCheckpointCommit func(),
	callback Callback,
) (Producer, error) {
	writer := kafkaClient.Producer()

	return Producer{
		ProducerBatch: newBatch(
			config.Kafka.ProducerBatchTickerDuration,
			writer,
			config.Kafka.ProducerBatchSize,
			int64(helpers.ResolveUnionIntOrStringValue(config.Kafka.ProducerBatchBytes)),
			dcpCheckpointCommit,
			callback,
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
