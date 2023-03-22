package config

import (
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type Kafka struct {
	CollectionTopicMapping      map[string]string `yaml:"collectionTopicMapping"`
	InterCAPath                 string            `yaml:"interCAPath"`
	ScramUsername               string            `yaml:"scramUsername"`
	ScramPassword               string            `yaml:"scramPassword"`
	RootCAPath                  string            `yaml:"rootCAPath"`
	Brokers                     []string          `yaml:"brokers"`
	ProducerBatchSize           int               `yaml:"producerBatchSize"`
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

type Config struct {
	Kafka *Kafka `yaml:"kafka"`
}

func NewConfig(filePath string) (*Config, error) {
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var c *Config
	err = yaml.Unmarshal(fileBytes, &c)
	if err != nil {
		return nil, err
	}

	return c, err
}
