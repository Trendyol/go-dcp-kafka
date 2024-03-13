package config

import (
	"math"
	"strconv"
	"time"

	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	ProducerBatchBytes          any               `yaml:"producerBatchBytes"`
	CollectionTopicMapping      map[string]string `yaml:"collectionTopicMapping"`
	InterCAPath                 string            `yaml:"interCAPath"`
	ScramUsername               string            `yaml:"scramUsername"`
	ScramPassword               string            `yaml:"scramPassword"`
	RootCAPath                  string            `yaml:"rootCAPath"`
	ClientID                    string            `yaml:"clientID"`
	Balancer                    string            `yaml:"balancer"`
	Brokers                     []string          `yaml:"brokers"`
	MetadataTopics              []string          `yaml:"metadataTopics"`
	ProducerMaxAttempts         int               `yaml:"producerMaxAttempts"`
	ProducerBatchTimeout        time.Duration     `yaml:"producerBatchTimeout"`
	ReadTimeout                 time.Duration     `yaml:"readTimeout"`
	WriteTimeout                time.Duration     `yaml:"writeTimeout"`
	RequiredAcks                int               `yaml:"requiredAcks"`
	ProducerBatchSize           int               `yaml:"producerBatchSize"`
	MetadataTTL                 time.Duration     `yaml:"metadataTTL"`
	ProducerBatchTickerDuration time.Duration     `yaml:"producerBatchTickerDuration"`
	Compression                 int8              `yaml:"compression"`
	SecureConnection            bool              `yaml:"secureConnection"`
	AllowAutoTopicCreation      bool              `yaml:"allowAutoTopicCreation"`
}

func (k *Kafka) GetBalancer() kafka.Balancer {
	switch k.Balancer {
	case "", "Hash":
		return &kafka.Hash{}
	case "LeastBytes":
		return &kafka.LeastBytes{}
	case "RoundRobin":
		return &kafka.RoundRobin{}
	case "ReferenceHash":
		return &kafka.ReferenceHash{}
	case "CRC32Balancer":
		return kafka.CRC32Balancer{}
	case "Murmur2Balancer":
		return kafka.Murmur2Balancer{}
	default:
		panic("invalid kafka balancer method, given: " + k.Balancer)
	}
}

func (k *Kafka) GetCompression() int8 {
	if k.Compression < 0 || k.Compression > 4 {
		panic("invalid kafka compression method, given: " + strconv.Itoa(int(k.Compression)))
	}
	return k.Compression
}

type Connector struct {
	Kafka Kafka      `yaml:"kafka" mapstructure:"kafka"`
	Dcp   config.Dcp `yaml:",inline" mapstructure:",squash"`
}

func (c *Connector) ApplyDefaults() {
	if c.Kafka.ReadTimeout == 0 {
		c.Kafka.ReadTimeout = 30 * time.Second
	}

	if c.Kafka.WriteTimeout == 0 {
		c.Kafka.WriteTimeout = 30 * time.Second
	}

	if c.Kafka.ProducerBatchTickerDuration == 0 {
		c.Kafka.ProducerBatchTickerDuration = 10 * time.Second
	}

	if c.Kafka.ProducerBatchSize == 0 {
		c.Kafka.ProducerBatchSize = 2000
	}

	if c.Kafka.ProducerBatchBytes == nil {
		c.Kafka.ProducerBatchBytes = helpers.ResolveUnionIntOrStringValue("10mb")
	}

	if c.Kafka.RequiredAcks == 0 {
		c.Kafka.RequiredAcks = 1
	}

	if c.Kafka.MetadataTTL == 0 {
		c.Kafka.MetadataTTL = 60 * time.Second
	}

	if c.Kafka.ProducerMaxAttempts == 0 {
		c.Kafka.ProducerMaxAttempts = math.MaxInt
	}

	if c.Kafka.ProducerBatchTimeout == 0 {
		c.Kafka.ProducerBatchTimeout = time.Nanosecond
	}
}
