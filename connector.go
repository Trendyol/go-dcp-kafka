package gokafkaconnectcouchbase

import (
	godcpclient "github.com/Trendyol/go-dcp-client"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/models"
	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/message"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/metadata"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/producer"
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
		e = couchbase.NewMutateEvent(event.Key, event.Value, event.CollectionName)
	case models.DcpExpiration:
		e = couchbase.NewExpireEvent(event.Key, nil, event.CollectionName)
	case models.DcpDeletion:
		e = couchbase.NewDeleteEvent(event.Key, nil, event.CollectionName)
	default:
		return
	}

	for _, kafkaMessage := range c.mapper(e) {
		if kafkaMessage != nil {
			topic := c.config.Kafka.CollectionTopicMapping[e.CollectionName]
			if topic == "" {
				c.errorLogger.Printf("unexpected collection | %s", e.CollectionName)
				return
			}
			c.producer.Produce(ctx, kafkaMessage.Value, kafkaMessage.Key, kafkaMessage.Headers, topic)
			message.MessagePool.Put(kafkaMessage)
		}
	}
}

func NewConnector(configPath string, mapper Mapper) (Connector, error) {
	return newConnector(configPath, mapper, logger.Log, logger.Log)
}

func NewConnectorWithLoggers(configPath string, mapper Mapper, logger logger.Logger, errorLogger logger.Logger) (Connector, error) {
	return newConnector(configPath, mapper, logger, errorLogger)
}

func newConnector(configPath string, mapper Mapper, logger logger.Logger, errorLogger logger.Logger) (Connector, error) {
	c := config.NewConfig("cbgokafka", configPath, errorLogger)

	connector := &connector{
		mapper:      mapper,
		config:      c,
		logger:      logger,
		errorLogger: errorLogger,
	}

	kafkaClient := kafka.NewClient(c.Kafka, connector.logger, connector.errorLogger)

	var topics []string

	for _, topic := range c.Kafka.CollectionTopicMapping {
		topics = append(topics, topic)
	}

	err := kafkaClient.CheckTopics(topics)
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

	if dcpConfig.Metadata.Type == MetadataTypeKafka {
		kafkaMetadata := metadata.NewKafkaMetadata(kafkaClient, dcpConfig.Metadata.Config, connector.logger, connector.errorLogger)
		dcp.SetMetadata(kafkaMetadata)
	}

	connector.dcp = dcp

	connector.producer, err = producer.NewProducer(kafkaClient, c.Kafka, connector.logger, connector.errorLogger, dcp.Commit)
	if err != nil {
		connector.errorLogger.Printf("kafka error: %v", err)
		return nil, err
	}
	return connector, nil
}
