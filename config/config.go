package config

import (
	"github.com/segmentio/kafka-go"
	"math"
	"time"

	"github.com/Trendyol/go-dcp/helpers"

	"github.com/Trendyol/go-dcp/config"
)

var balancerMap = map[string]kafka.Balancer{
	"":                &kafka.Hash{},
	"Hash":            &kafka.Hash{},
	"RoundRobin":      &kafka.RoundRobin{},
	"LeastBytes":      &kafka.LeastBytes{},
	"ReferenceHash":   &kafka.ReferenceHash{},
	"CRC32Balancer":   &kafka.CRC32Balancer{},
	"Murmur2Balancer": &kafka.Murmur2Balancer{},
}

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
	if balancer, ok := balancerMap[k.Balancer]; ok {
		return balancer
	}
	panic("Invalid kafka balancer method")
}

func (k *Kafka) GetCompression() int8 {
	if k.Compression < 0 || k.Compression > 4 {
		panic("Invalid kafka compression method")
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
