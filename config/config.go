package config

import (
	"time"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
)

type Kafka struct {
	CollectionTopicMapping      map[string]string `yaml:"collectionTopicMapping"`
	InterCAPath                 string            `yaml:"interCAPath"`
	ScramUsername               string            `yaml:"scramUsername"`
	ScramPassword               string            `yaml:"scramPassword"`
	RootCAPath                  string            `yaml:"rootCAPath"`
	Brokers                     []string          `yaml:"brokers"`
	ProducerBatchSize           int               `yaml:"producerBatchSize" default:"2000"`
	ProducerBatchBytes          int               `yaml:"producerBatchBytes" default:"10485760"`
	ProducerBatchTickerDuration time.Duration     `yaml:"producerBatchTickerDuration"`
	ReadTimeout                 time.Duration     `yaml:"readTimeout"`
	WriteTimeout                time.Duration     `yaml:"writeTimeout"`
	RequiredAcks                int               `yaml:"requiredAcks" default:"1"`
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
	Kafka Kafka `yaml:"kafka"`
}

func Options(opts *config.Options) {
	opts.ParseTime = true
	opts.Readonly = true
	opts.EnableCache = true
	opts.ParseDefault = true
}

func applyUnhandledDefaults(_config *Config) {
	if _config.Kafka.ReadTimeout == 0 {
		_config.Kafka.ReadTimeout = 30 * time.Second
	}

	if _config.Kafka.WriteTimeout == 0 {
		_config.Kafka.WriteTimeout = 30 * time.Second
	}

	if _config.Kafka.ProducerBatchTickerDuration == 0 {
		_config.Kafka.ProducerBatchTickerDuration = 10 * time.Second
	}
}

func NewConfig(name string, filePath string, errorLogger logger.Logger) *Config {
	conf := config.New(name).WithOptions(Options).WithDriver(yamlv3.Driver)

	err := conf.LoadFiles(filePath)
	if err != nil {
		errorLogger.Printf("error while reading config %v", err)
	}

	_config := &Config{}
	err = conf.Decode(_config)

	if err != nil {
		errorLogger.Printf("error while reading config %v", err)
	}

	applyUnhandledDefaults(_config)

	return _config
}
