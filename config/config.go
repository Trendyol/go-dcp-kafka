package config

import (
	"time"

	"github.com/Trendyol/go-dcp-client/config"
)

type Kafka struct {
	CollectionTopicMapping      map[string]string `yaml:"collectionTopicMapping"`
	InterCAPath                 string            `yaml:"interCAPath"`
	ScramUsername               string            `yaml:"scramUsername"`
	ScramPassword               string            `yaml:"scramPassword"`
	RootCAPath                  string            `yaml:"rootCAPath"`
	Brokers                     []string          `yaml:"brokers"`
	ProducerBatchSize           int               `yaml:"producerBatchSize"`
	ProducerBatchBytes          int               `yaml:"producerBatchBytes"`
	ProducerBatchTickerDuration time.Duration     `yaml:"producerBatchTickerDuration"`
	ReadTimeout                 time.Duration     `yaml:"readTimeout"`
	WriteTimeout                time.Duration     `yaml:"writeTimeout"`
	RequiredAcks                int               `yaml:"requiredAcks"`
	Compression                 int8              `yaml:"compression"`
	SecureConnection            bool              `yaml:"secureConnection"`
}

func (k *Kafka) GetCompression() int8 {
	if k.Compression < 0 || k.Compression > 4 {
		panic("Invalid kafka compression method")
	}
	return k.Compression
}

type Connector struct {
	Kafka Kafka `yaml:"Kafka"`
	Dcp   config.Dcp
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

	if c.Kafka.ProducerBatchBytes == 0 {
		c.Kafka.ProducerBatchBytes = 10485760
	}

	if c.Kafka.RequiredAcks == 0 {
		c.Kafka.RequiredAcks = 1
	}
}
