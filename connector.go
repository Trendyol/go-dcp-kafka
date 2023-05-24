package gokafkaconnectcouchbase

import (
	"errors"
	"os"

	"github.com/Trendyol/go-dcp-client"
	dcpClientConfig "github.com/Trendyol/go-dcp-client/config"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/models"
	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/couchbase"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/metadata"
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
		return newConnectorFromConfig(cfg, v, mapper)
	case string:
		return newConnectorFromPath(v, mapper)
	default:
		return nil, errors.New("invalid config")
	}
}

func NewConnectorWithLoggers(configPath string, mapper Mapper, infoLogger logger.Logger, errorLogger logger.Logger) (Connector, error) {
	logger.SetLogger(infoLogger)
	logger.SetErrorLogger(errorLogger)

	return NewConnector(configPath, mapper)
}

func newConnectorFromConfig(cfg any, cc *config.Connector, mapper Mapper) (Connector, error) {
	connector := &connector{
		mapper:      mapper,
		config:      cc,
		logger:      logger.Log,
		errorLogger: logger.Log,
	}

	kafkaClient, err := createKafkaClient(cc, connector)
	if err != nil {
		return nil, err
	}

	dcp, err := createDcp(cfg, connector.produce, connector.logger, connector.errorLogger)
	if err != nil {
		connector.errorLogger.Printf("dcp error: %v", err)
		return nil, err
	}

	if dcpConfig := dcp.GetConfig(); dcpConfig.Metadata.Type == MetadataTypeKafka {
		setKafkaMetadata(kafkaClient, dcpConfig, connector, dcp)
	}

	connector.dcp = dcp

	connector.producer, err = producer.NewProducer(kafkaClient, cc, connector.logger, connector.errorLogger, dcp.Commit)
	if err != nil {
		connector.errorLogger.Printf("kafka error: %v", err)
		return nil, err
	}

	initializeMetricCollector(connector, dcp)

	return connector, nil
}

func createKafkaClient(cc *config.Connector, connector *connector) (kafka.Client, error) {
	kafkaClient := kafka.NewClient(cc, connector.logger, connector.errorLogger)

	var topics []string

	for _, topic := range cc.Kafka.CollectionTopicMapping {
		topics = append(topics, topic)
	}

	if err := kafkaClient.CheckTopics(topics); err != nil {
		connector.errorLogger.Printf("collection topic mapping error: %v", err)
		return nil, err
	}
	return kafkaClient, nil
}

func createDcp(cfg any, listener models.Listener, logger logger.Logger, errorLogger logger.Logger) (godcpclient.Dcp, error) {
	switch v := cfg.(type) {
	case dcpClientConfig.Dcp:
		return godcpclient.NewDcpWithLoggers(v.Dcp, listener, logger, errorLogger)
	case string:
		return godcpclient.NewDcpWithLoggers(v, listener, logger, errorLogger)
	default:
		return nil, errors.New("invalid config")
	}
}

func setKafkaMetadata(kafkaClient kafka.Client, dcpConfig *dcpClientConfig.Dcp, connector *connector, dcp godcpclient.Dcp) {
	kafkaMetadata := metadata.NewKafkaMetadata(kafkaClient, dcpConfig.Metadata.Config, connector.logger, connector.errorLogger)
	dcp.SetMetadata(kafkaMetadata)
}

func initializeMetricCollector(connector *connector, dcp godcpclient.Dcp) {
	metricCollector := metric.NewMetricCollector(connector.producer)
	dcp.SetMetricCollectors(metricCollector)
}

func newConnectorFromPath(path string, mapper Mapper) (Connector, error) {
	c, err := newConnectorConfig(path)
	if err != nil {
		return nil, err
	}
	return newConnectorFromConfig(path, c, mapper)
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
