package config

import (
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
	"log"
	"time"
)

type Kafka struct {
	Brokers                     string        `yaml:"brokers"`
	Topic                       string        `yaml:"topic"`
	ProducerBatchSize           int           `yaml:"producerBatchSize"`
	ProducerBatchTickerDuration time.Duration `yaml:"producerBatchTickerDuration"`
	MaxAttempts                 int           `yaml:"maxAttempts"`
	ReadTimeout                 time.Duration `yaml:"readTimeout"`
	WriteTimeout                time.Duration `yaml:"writeTimeout"`
	RequiredAcks                string        `yaml:"requiredAcks"`
}

type Config struct {
	Kafka *Kafka `yaml:"kafka"`
}

func Options(opts *config.Options) {
	opts.ParseTime = true
	opts.Readonly = true
	opts.EnableCache = true
}

func NewConfig(name string, filePath string) *Config {
	conf := config.New(name).WithOptions(Options).WithDriver(yamlv3.Driver)

	err := conf.LoadFiles(filePath)

	if err != nil {
		panic(err)
	}

	_config := &Config{}
	err = conf.Decode(_config)

	if err != nil {
		panic(err)
	}

	log.Printf("config loaded from file: %v", filePath)

	return _config
}
