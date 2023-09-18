package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"time"

	"github.com/segmentio/kafka-go/sasl"

	"github.com/Trendyol/go-dcp-kafka/config"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Client interface {
	GetEndOffsets(topic string, partitions []int) ([]kafka.PartitionOffsets, error)
	GetPartitions(topic string) ([]int, error)
	CreateCompactedTopic(topic string, partition int, replicationFactor int) error
	Producer() *kafka.Writer
	Consumer(topic string, partition int, startOffset int64) *kafka.Reader
	CheckTopicIsCompacted(topic string) error
	CheckTopics(topics []string) error
}

type client struct {
	addr        net.Addr
	kafkaClient *kafka.Client
	config      *config.Connector
	logger      logger.Logger
	errorLogger logger.Logger
	transport   *kafka.Transport
	dialer      *kafka.Dialer
}

type tlsContent struct {
	config *tls.Config
	sasl   sasl.Mechanism
}

func newTLSContent(
	scramUsername,
	scramPassword,
	rootCAPath,
	interCAPath string,
	errorLogger logger.Logger,
) (*tlsContent, error) {
	mechanism, err := scram.Mechanism(scram.SHA512, scramUsername, scramPassword)
	if err != nil {
		return nil, err
	}

	caCert, err := os.ReadFile(os.ExpandEnv(rootCAPath))
	if err != nil {
		errorLogger.Printf("an error occurred while reading ca.pem file! Error: %s", err.Error())
		return nil, err
	}

	intCert, err := os.ReadFile(os.ExpandEnv(interCAPath))
	if err != nil {
		errorLogger.Printf("an error occurred while reading int.pem file! Error: %s", err.Error())
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	caCertPool.AppendCertsFromPEM(intCert)

	return &tlsContent{
		config: &tls.Config{
			RootCAs:    caCertPool,
			MinVersion: tls.VersionTLS12,
		},
		sasl: mechanism,
	}, nil
}

func (c *client) GetEndOffsets(topic string, partitions []int) ([]kafka.PartitionOffsets, error) {
	var offsetRequests []kafka.OffsetRequest

	for _, partition := range partitions {
		offsetRequests = append(offsetRequests, kafka.LastOffsetOf(partition))
	}

	request := &kafka.ListOffsetsRequest{
		Addr: c.addr,
		Topics: map[string][]kafka.OffsetRequest{
			topic: offsetRequests,
		},
		IsolationLevel: kafka.ReadUncommitted,
	}

	response, err := c.kafkaClient.ListOffsets(context.Background(), request)
	if err != nil {
		return nil, err
	}

	return response.Topics[topic], nil
}

func (c *client) CheckTopicIsCompacted(topic string) error {
	response, err := c.kafkaClient.DescribeConfigs(context.Background(), &kafka.DescribeConfigsRequest{
		Addr: c.addr,
		Resources: []kafka.DescribeConfigRequestResource{
			{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: topic,
				ConfigNames:  []string{"cleanup.policy"},
			},
		},
		IncludeSynonyms:      false,
		IncludeDocumentation: false,
	})
	if err != nil {
		return err
	}

	for _, resource := range response.Resources {
		if resource.Error != nil {
			return resource.Error
		}

		if resource.ResourceType == int8(kafka.ResourceTypeTopic) {
			for _, entity := range resource.ConfigEntries {
				if entity.ConfigName == "cleanup.policy" && entity.ConfigValue == "compact" {
					return nil
				}
			}
		}
	}

	return errors.New("topic is not compacted")
}

func (c *client) GetPartitions(topic string) ([]int, error) {
	response, err := c.kafkaClient.Metadata(context.Background(), &kafka.MetadataRequest{
		Topics: []string{topic},
		Addr:   c.addr,
	})
	if err != nil {
		return nil, err
	}

	var partitions []int

	for _, responseTopic := range response.Topics {
		if responseTopic.Name == topic {
			for _, partition := range responseTopic.Partitions {
				partitions = append(partitions, partition.ID)
			}
		}
	}

	return partitions, nil
}

func (c *client) CheckTopics(topics []string) error {
	response, err := c.kafkaClient.Metadata(context.Background(), &kafka.MetadataRequest{
		Topics: topics,
		Addr:   c.addr,
	})
	if err != nil {
		return err
	}

	for _, responseTopic := range response.Topics {
		if responseTopic.Error != nil {
			return fmt.Errorf("topic=%s, err=%v", responseTopic.Name, responseTopic.Error)
		}
	}

	return nil
}

func (c *client) Producer() *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(c.config.Kafka.Brokers...),
		Balancer:               &kafka.Hash{},
		BatchSize:              c.config.Kafka.ProducerBatchSize,
		BatchBytes:             math.MaxInt,
		BatchTimeout:           500 * time.Microsecond,
		MaxAttempts:            math.MaxInt,
		ReadTimeout:            c.config.Kafka.ReadTimeout,
		WriteTimeout:           c.config.Kafka.WriteTimeout,
		RequiredAcks:           kafka.RequiredAcks(c.config.Kafka.RequiredAcks),
		Logger:                 c.logger,
		ErrorLogger:            c.errorLogger,
		Compression:            kafka.Compression(c.config.Kafka.GetCompression()),
		Transport:              c.transport,
		AllowAutoTopicCreation: c.config.Kafka.AllowAutoTopicCreation,
	}
}

func (c *client) Consumer(topic string, partition int, startOffset int64) *kafka.Reader {
	readerConfig := kafka.ReaderConfig{
		Brokers:     c.config.Kafka.Brokers,
		Topic:       topic,
		Partition:   partition,
		StartOffset: startOffset,
	}

	if c.dialer != nil {
		readerConfig.Dialer = c.dialer
	}

	return kafka.NewReader(readerConfig)
}

func (c *client) CreateCompactedTopic(topic string, partition int, replicationFactor int) error {
	response, err := c.kafkaClient.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     partition,
				ReplicationFactor: replicationFactor,
				ConfigEntries: []kafka.ConfigEntry{{
					ConfigName:  "cleanup.policy",
					ConfigValue: "compact",
				}, {
					ConfigName:  "min.insync.replicas",
					ConfigValue: "2",
				}, {
					ConfigName:  "retention.ms",
					ConfigValue: "86400000",
				}, {
					ConfigName:  "max.message.bytes",
					ConfigValue: "1048588",
				}, {
					ConfigName:  "retention.bytes",
					ConfigValue: "-1",
				}, {
					ConfigName:  "segment.bytes",
					ConfigValue: "2097152", // 2MB
				}},
			},
		},
	})
	if err != nil {
		return err
	}

	for _, topicError := range response.Errors {
		if topicError != nil {
			return topicError
		}
	}

	return nil
}

func NewClient(config *config.Connector, logger logger.Logger, errorLogger logger.Logger) Client {
	addr := kafka.TCP(config.Kafka.Brokers...)

	newClient := &client{
		addr: addr,
		kafkaClient: &kafka.Client{
			Addr: addr,
		},
		config:      config,
		logger:      logger,
		errorLogger: errorLogger,
	}

	newClient.transport = &kafka.Transport{
		MetadataTTL:    config.Kafka.MetadataTTL,
		MetadataTopics: config.Kafka.MetadataTopics,
		ClientID:       config.Kafka.ClientID,
	}

	if config.Kafka.SecureConnection {
		tlsContent, err := newTLSContent(
			config.Kafka.ScramUsername,
			config.Kafka.ScramPassword,
			config.Kafka.RootCAPath,
			config.Kafka.InterCAPath,
			errorLogger,
		)
		if err != nil {
			panic(err)
		}

		newClient.transport.TLS = tlsContent.config
		newClient.transport.SASL = tlsContent.sasl

		newClient.dialer = &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			TLS:           tlsContent.config,
			SASLMechanism: tlsContent.sasl,
		}
	}
	newClient.kafkaClient.Transport = newClient.transport
	return newClient
}
