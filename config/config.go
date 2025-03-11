package config

import (
	"errors"
	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/Trendyol/go-dcp/logger"
	"math"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	ProducerBatchBytes          any               `yaml:"producerBatchBytes"`
	CollectionTopicMapping      map[string]string `yaml:"collectionTopicMapping"`
	InterCAPath                 string            `yaml:"interCAPath"`
	InterCA                     string            `yaml:"interCA"`
	ScramUsername               string            `yaml:"scramUsername"`
	ScramPassword               string            `yaml:"scramPassword"`
	RootCAPath                  string            `yaml:"rootCAPath"`
	RootCA                      string            `yaml:"rootCA"`
	ClientID                    string            `yaml:"clientID"`
	Balancer                    string            `yaml:"balancer"`
	Brokers                     []string          `yaml:"brokers"`
	MetadataTopics              []string          `yaml:"metadataTopics"`
	RejectionLog                RejectionLog      `yaml:"rejectionLog"`
	ProducerMaxAttempts         int               `yaml:"producerMaxAttempts"`
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

type RejectionLog struct {
	Topic        string `yaml:"topic"`
	IncludeValue bool   `yaml:"includeValue"`
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
		err := errors.New("invalid kafka balancer method, given: " + k.Balancer)
		logger.Log.Error("error while get kafka balancer, err: %v", err)
		panic(err)
	}
}

func (k *Kafka) GetCompression() int8 {
	if k.Compression < 0 || k.Compression > 4 {
		err := errors.New("invalid kafka compression method, given: " + strconv.Itoa(int(k.Compression)))
		logger.Log.Error("error while get kafka compression, err: %v", err)
		panic(err)
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
		c.Kafka.ProducerBatchBytes = helpers.ResolveUnionIntOrStringValue("1mb")
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
}
