package config

import (
	"time"

	"github.com/Trendyol/go-kafka-connect-couchbase/logger"

	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
)

type Kafka struct {
	Brokers                     string        `yaml:"brokers"`
	Topic                       string        `yaml:"topic"`
	ProducerBatchSize           int           `yaml:"producerBatchSize"`
	ProducerBatchTickerDuration time.Duration `yaml:"producerBatchTickerDuration"`
	ReadTimeout                 time.Duration `yaml:"readTimeout"`
	WriteTimeout                time.Duration `yaml:"writeTimeout"`
	RequiredAcks                int           `yaml:"requiredAcks"`
}

type Config struct {
	Kafka *Kafka `yaml:"kafka"`
}

func Options(opts *config.Options) {
	opts.ParseTime = true
	opts.Readonly = true
	opts.EnableCache = true
}

func NewConfig(name string, filePath string, errorLogger logger.Logger) *Config {
	conf := config.New(name).WithOptions(Options).WithDriver(yamlv3.Driver)

	err := conf.LoadFiles(filePath)
	if err != nil {
		errorLogger.Printf("Error while reading config %v", err)
	}

	_config := &Config{}
	err = conf.Decode(_config)

	if err != nil {
		errorLogger.Printf("Error while reading config %v", err)
	}

	return _config
}
