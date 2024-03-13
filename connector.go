package dcpkafka

import (
	"errors"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/Trendyol/go-dcp"

	"github.com/Trendyol/go-dcp-kafka/config"
	"github.com/Trendyol/go-dcp-kafka/couchbase"
	"github.com/Trendyol/go-dcp-kafka/kafka"
	"github.com/Trendyol/go-dcp-kafka/kafka/metadata"
	"github.com/Trendyol/go-dcp-kafka/kafka/producer"
	"github.com/Trendyol/go-dcp-kafka/metric"
	dcpConfig "github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
	sKafka "github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
)

var MetadataTypeKafka = "kafka"

type Connector interface {
	Start()
	Close()
}

type connector struct {
	dcp      dcp.Dcp
	mapper   Mapper
	producer producer.Producer
	config   *config.Connector
}

func (c *connector) Start() {
	go func() {
		<-c.dcp.WaitUntilReady()
		c.producer.StartBatch()
	}()
	c.dcp.Start()
}

func (c *connector) Close() {
	c.dcp.Close()
	err := c.producer.Close()
	if err != nil {
		logger.Log.Error("error | %v", err)
	}
}

func (c *connector) produce(ctx *models.ListenerContext) {
	var e couchbase.Event
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		e = couchbase.NewMutateEvent(event.Key, event.Value, event.CollectionName, event.EventTime, event.Cas, event.VbID)
	case models.DcpExpiration:
		e = couchbase.NewExpireEvent(event.Key, nil, event.CollectionName, event.EventTime, event.Cas, event.VbID)
	case models.DcpDeletion:
		e = couchbase.NewDeleteEvent(event.Key, nil, event.CollectionName, event.EventTime, event.Cas, event.VbID)
	default:
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
			Topic:   c.getTopicName(e.CollectionName, message.Topic),
			Key:     message.Key,
			Value:   message.Value,
			Headers: message.Headers,
		})
	}
	c.producer.Produce(ctx, e.EventTime, messages)
}

func (c *connector) getTopicName(collectionName string, messageTopic string) string {
	if messageTopic != "" {
		return messageTopic
	}

	topic := c.config.Kafka.CollectionTopicMapping[collectionName]
	if topic == "" {
		panic(fmt.Sprintf("there is no topic mapping for collection: %s on your configuration", collectionName))
	}
	return topic
}

func newConnector(cfg any, mapper Mapper, sinkResponseHandler kafka.SinkResponseHandler) (Connector, error) {
	c, err := newConfig(cfg)
	if err != nil {
		return nil, err
	}
	c.ApplyDefaults()

	connector := &connector{
		mapper: mapper,
		config: c,
	}

	dcpClient, err := dcp.NewDcp(&c.Dcp, connector.produce)
	if err != nil {
		logger.Log.Error("dcp error: %v", err)
		return nil, err
	}

	conf := dcpClient.GetConfig()
	conf.Checkpoint.Type = "manual"

	kafkaClient, err := createKafkaClient(c)
	if err != nil {
		return nil, err
	}

	if conf.Metadata.Type == MetadataTypeKafka {
		setKafkaMetadata(kafkaClient, conf, dcpClient)
	}

	connector.dcp = dcpClient

	connector.producer, err = producer.NewProducer(kafkaClient, c, dcpClient.Commit, sinkResponseHandler)
	if err != nil {
		logger.Log.Error("kafka error: %v", err)
		return nil, err
	}

	connector.dcp.SetEventHandler(&DcpEventHandler{
		producerBatch: connector.producer.ProducerBatch,
	})

	initializeMetricCollector(connector, dcpClient)

	return connector, nil
}

func newConfig(cf any) (*config.Connector, error) {
	switch v := cf.(type) {
	case *config.Connector:
		return v, nil
	case config.Connector:
		return &v, nil
	case string:
		return newConnectorConfigFromPath(v)
	default:
		return nil, errors.New("invalid config")
	}
}

func createKafkaClient(cc *config.Connector) (kafka.Client, error) {
	kafkaClient := kafka.NewClient(cc)

	var topics []string

	for _, topic := range cc.Kafka.CollectionTopicMapping {
		topics = append(topics, topic)
	}

	if !cc.Kafka.AllowAutoTopicCreation {
		if err := kafkaClient.CheckTopics(topics); err != nil {
			logger.Log.Error("collection topic mapping error: %v", err)
			return nil, err
		}
	}

	return kafkaClient, nil
}

func setKafkaMetadata(kafkaClient kafka.Client, dcpConfig *dcpConfig.Dcp, dcp dcp.Dcp) {
	kafkaMetadata := metadata.NewKafkaMetadata(kafkaClient, dcpConfig.Metadata.Config)
	dcp.SetMetadata(kafkaMetadata)
}

func initializeMetricCollector(connector *connector, dcp dcp.Dcp) {
	metricCollector := metric.NewMetricCollector(connector.producer)
	dcp.SetMetricCollectors(metricCollector)
}

func newConnectorConfigFromPath(path string) (*config.Connector, error) {
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

type ConnectorBuilder struct {
	mapper              Mapper
	config              any
	sinkResponseHandler kafka.SinkResponseHandler
}

func NewConnectorBuilder(config any) *ConnectorBuilder {
	return &ConnectorBuilder{
		config:              config,
		mapper:              DefaultMapper,
		sinkResponseHandler: nil,
	}
}

func (c *ConnectorBuilder) SetMapper(mapper Mapper) *ConnectorBuilder {
	c.mapper = mapper
	return c
}

func (c *ConnectorBuilder) SetSinkResponseHandler(sinkResponseHandler kafka.SinkResponseHandler) *ConnectorBuilder {
	c.sinkResponseHandler = sinkResponseHandler
	return c
}

func (c *ConnectorBuilder) Build() (Connector, error) {
	return newConnector(c.config, c.mapper, c.sinkResponseHandler)
}

func (c *ConnectorBuilder) SetLogger(l *logrus.Logger) *ConnectorBuilder {
	logger.Log = &logger.Loggers{
		Logrus: l,
	}
	return c
}
