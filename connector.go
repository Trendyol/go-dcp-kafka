package godcpkafkaconnector

import (
	"context"
	godcpclient "github.com/Trendyol/go-dcp-client"
	"godcpkafkaconnector/config"
	"godcpkafkaconnector/couchbase"
	kafka "godcpkafkaconnector/kafka/producer"
	"godcpkafkaconnector/logger"
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

func (c *connector) listener(event interface{}, err error) {
	if err != nil {
		c.errorLogger.Printf("error | %v", err)
		return
	}

	var e *couchbase.Event
	switch event := event.(type) {
	case godcpclient.DcpMutation:
		e = couchbase.NewMutateEvent(event.Key, event.Value)
	case godcpclient.DcpExpiration:
		e = couchbase.NewExpireEvent(event.Key, nil)
	case godcpclient.DcpDeletion:
		e = couchbase.NewDeleteEvent(event.Key, nil)
	default:
		return
	}

	if message := c.mapper(e); message != nil {
		// TODO: use contexts
		ctx := context.TODO()
		err = c.producer.Produce(&ctx, message.Value, message.Key, message.Headers)
		if err != nil {
			c.errorLogger.Printf("error | %v", err)
		}
	}

}

func NewConnector(configPath string, mapper Mapper) Connector {
	return newConnector(configPath, mapper, &logger.Log, &logger.Log)
}

func NewConnectorWithLoggers(configPath string, mapper Mapper, logger logger.Logger, errorLogger logger.Logger) Connector {
	return newConnector(configPath, mapper, logger, errorLogger)
}

func newConnector(configPath string, mapper Mapper, logger logger.Logger, errorLogger logger.Logger) Connector {
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
	connector.producer = kafka.NewProducer(c.Kafka, connector.logger, connector.errorLogger)
	return connector
}
