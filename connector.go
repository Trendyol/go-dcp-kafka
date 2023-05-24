package gokafkaconnectcouchbase

import (
	"os"

	"github.com/Trendyol/go-dcp-client"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/models"
	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/metadata"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/producer"
	"github.com/Trendyol/go-kafka-connect-couchbase/metric"
	sKafka "github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
)

var MetadataTypeKafka = "kafka"

type Connector interface {
	Start()
	Close()
}

type connector struct {
	dcp         godcpclient.Dcp
	mapper      Mapper
	producer    producer.Producer
	config      *config.Config
	logger      logger.Logger
	errorLogger logger.Logger
}

func (c *connector) Start() {
	c.dcp.Start()
}

func (c *connector) Close() {
	c.dcp.Close()
	err := c.producer.Close()
	if err != nil {
		c.errorLogger.Printf("error | %v", err)
	}
}

func (c *connector) produce(ctx *models.ListenerContext) {
	var e couchbase.Event
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		e = couchbase.NewMutateEvent(event.Key, event.Value, event.CollectionName, event.EventTime)
	case models.DcpExpiration:
		e = couchbase.NewExpireEvent(event.Key, nil, event.CollectionName, event.EventTime)
	case models.DcpDeletion:
		e = couchbase.NewDeleteEvent(event.Key, nil, event.CollectionName, event.EventTime)
	default:
		return
	}
	topic := c.config.Kafka.CollectionTopicMapping[e.CollectionName]
	if topic == "" {
		c.errorLogger.Printf("unexpected collection | %s", e.CollectionName)
		return
	}
	kafkaMessages := c.mapper(e)

	if len(kafkaMessages) == 0 {
		ctx.Ack()
		return
	}

	messages := make([]sKafka.Message, 0, len(kafkaMessages))
	for _, message := range kafkaMessages {
		messages = append(messages, sKafka.Message{
			Topic:   topic,
			Key:     message.Key,
			Value:   message.Value,
			Headers: message.Headers,
		})
	}
	c.producer.Produce(ctx, e.EventTime, messages)
}

func NewConnector(configPath string, mapper Mapper) (Connector, error) {
	return newConnector(configPath, mapper, logger.Log, logger.Log)
}

func NewConnectorWithLoggers(configPath string, mapper Mapper, logger logger.Logger, errorLogger logger.Logger) (Connector, error) {
	return newConnector(configPath, mapper, logger, errorLogger)
}

func newConnectorConfig(path string) (*config.Config, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c config.Config
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return nil, err
	}
	c.ApplyDefaults()
	return &c, nil
}

func newConnector(configPath string, mapper Mapper, logger logger.Logger, errorLogger logger.Logger) (Connector, error) {
	c, err := newConnectorConfig(configPath)
	if err != nil {
		return nil, err
	}

	connector := &connector{
		mapper:      mapper,
		config:      c,
		logger:      logger,
		errorLogger: errorLogger,
	}

	kafkaClient := kafka.NewClient(c, connector.logger, connector.errorLogger)

	var topics []string

	for _, topic := range c.Kafka.CollectionTopicMapping {
		topics = append(topics, topic)
	}

	err = kafkaClient.CheckTopics(topics)
	if err != nil {
		connector.errorLogger.Printf("collection topic mapping error: %v", err)
		return nil, err
	}

	dcp, err := godcpclient.NewDcpWithLoggers(configPath, connector.produce, logger, errorLogger)
	if err != nil {
		connector.errorLogger.Printf("dcp error: %v", err)
		return nil, err
	}

	dcpConfig := dcp.GetConfig()
	dcpConfig.Checkpoint.Type = "manual"

	if dcpConfig.Metadata.Type == MetadataTypeKafka {
		kafkaMetadata := metadata.NewKafkaMetadata(kafkaClient, dcpConfig.Metadata.Config, connector.logger, connector.errorLogger)
		dcp.SetMetadata(kafkaMetadata)
	}

	connector.dcp = dcp

	connector.producer, err = producer.NewProducer(kafkaClient, c, connector.logger, connector.errorLogger, dcp.Commit)
	if err != nil {
		connector.errorLogger.Printf("kafka error: %v", err)
		return nil, err
	}

	metricCollector := metric.NewMetricCollector(connector.producer)
	dcp.SetMetricCollectors(metricCollector)

	return connector, nil
}
