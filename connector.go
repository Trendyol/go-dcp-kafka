package gokafkaconnectcouchbase

import (
	"errors"
	"os"

	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/metadata"

	"github.com/Trendyol/go-dcp-client"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/models"
	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/producer"
	"github.com/Trendyol/go-kafka-connect-couchbase/metric"
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
	config      *config.Connector
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
	for i := range kafkaMessages {
		c.producer.Produce(ctx, e.EventTime, kafkaMessages[i].Value, kafkaMessages[i].Key, kafkaMessages[i].Headers, topic)
	}
}

func NewConnector(cfg any, mapper Mapper) (Connector, error) {
	switch v := cfg.(type) {
	case *config.Connector:
		return newConnector(cfg, v, mapper, logger.Log, logger.Log)
	case string:
		return newConnectorWithPath(v, mapper, logger.Log, logger.Log)
	default:
		return nil, errors.New("invalid config")
	}
}

func NewConnectorWithLoggers(configPath string, mapper Mapper, infoLogger logger.Logger, errorLogger logger.Logger) (Connector, error) {
	logger.SetLogger(infoLogger)
	logger.SetErrorLogger(errorLogger)

	return NewConnector(configPath, mapper)
}

func newConnector(
	cfg any,
	cc *config.Connector,
	mapper Mapper,
	logger logger.Logger,
	errorLogger logger.Logger,
) (Connector, error) {
	connector := &connector{
		mapper:      mapper,
		config:      cc,
		logger:      logger,
		errorLogger: errorLogger,
	}

	kafkaClient := kafka.NewClient(cc, connector.logger, connector.errorLogger)

	var topics []string

	for _, topic := range cc.Kafka.CollectionTopicMapping {
		topics = append(topics, topic)
	}

	err := kafkaClient.CheckTopics(topics)
	if err != nil {
		connector.errorLogger.Printf("collection topic mapping error: %v", err)
		return nil, err
	}

	switch v := cfg.(type) {
	case *config.Connector:
		dcp, err := godcpclient.NewDcpWithLoggers(v.Dcp, connector.produce, logger, errorLogger)
		if err != nil {
			connector.errorLogger.Printf("dcp error: %v", err)
			return nil, err
		}
	case string:
		dcp, err := godcpclient.NewDcpWithLoggers(v, connector.produce, logger, errorLogger)
		if err != nil {
			connector.errorLogger.Printf("dcp error: %v", err)
			return nil, err
		}
	default:
		return nil, errors.New("invalid config")
	}

	dcpConfig := dcp.GetConfig()

	if dcpConfig.Metadata.Type == MetadataTypeKafka {
		kafkaMetadata := metadata.NewKafkaMetadata(kafkaClient, dcpConfig.Metadata.Config, connector.logger, connector.errorLogger)
		dcp.SetMetadata(kafkaMetadata)
	}

	connector.dcp = dcp

	connector.producer, err = producer.NewProducer(kafkaClient, cc, connector.logger, connector.errorLogger, dcp.Commit)
	if err != nil {
		connector.errorLogger.Printf("kafka error: %v", err)
		return nil, err
	}

	metricCollector := metric.NewMetricCollector(connector.producer)
	dcp.SetMetricCollectors(metricCollector)

	return connector, nil
}

func newConnectorWithPath(path string, mapper Mapper, logger logger.Logger, errorLogger logger.Logger) (Connector, error) {
	c, err := newConnectorConfig(path)
	if err != nil {
		return nil, err
	}
	return newConnector(c, mapper, logger, errorLogger)
}

func newConnectorConfig(path string) (*config.Connector, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c config.Connector
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return nil, err
	}
	c.ApplyDefaults()
	return &c, nil
}

