package gokafkaconnectcouchbase

import (
	"github.com/Trendyol/go-dcp-client/models"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/message"

	godcpclient "github.com/Trendyol/go-dcp-client"
	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	kafka "github.com/Trendyol/go-kafka-connect-couchbase/kafka/producer"
	"github.com/Trendyol/go-kafka-connect-couchbase/logger"
)

type Connector interface {
	Start()
	Close()
}

type connector struct {
	dcp         godcpclient.Dcp
	mapper      Mapper
	producer    kafka.Producer
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

func (c *connector) listener(ctx *models.ListenerContext) {
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

	if kafkaMessage := c.mapper(e); kafkaMessage != nil {
		defer message.MessagePool.Put(kafkaMessage)
		topic := c.config.Kafka.CollectionTopicMapping[e.CollectionName]
		if topic == "" {
			c.errorLogger.Printf("unexpected collection | %s", e.CollectionName)
			return
		}
		c.producer.Produce(ctx, kafkaMessage.Value, kafkaMessage.Key, kafkaMessage.Headers, topic)
	}
}

func NewConnector(configPath string, mapper Mapper) (Connector, error) {
	return newConnector(configPath, mapper, &logger.Log, &logger.Log)
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

	dcp, err := godcpclient.NewDcp(configPath, connector.listener)
	if err != nil {
		connector.errorLogger.Printf("Dcp error: %v", err)
	}

	connector.dcp = dcp
	connector.producer = kafka.NewProducer(c.Kafka, connector.logger, connector.errorLogger, dcp.Commit)
	return connector, err
}
