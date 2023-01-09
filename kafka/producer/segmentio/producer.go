package segmentio

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/producer"
	"math"
	"os"
	"time"

	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/logger"

	"github.com/segmentio/kafka-go"
)

type segmentio struct {
	producerBatch *producerBatch
}

func NewProducer(config *config.Kafka, logger logger.Logger, errorLogger logger.Logger) producer.Producer {
	writer := &kafka.Writer{
		Topic:        config.Topic,
		Addr:         kafka.TCP(config.Brokers...),
		Balancer:     &kafka.Hash{},
		BatchSize:    config.ProducerBatchSize,
		BatchBytes:   math.MaxInt,
		BatchTimeout: config.ProducerBatchTimeout,
		MaxAttempts:  math.MaxInt,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(config.RequiredAcks),
		Logger:       logger,
		ErrorLogger:  errorLogger,
	}
	if config.SecureConnection {
		transport, err := createSecureKafkaTransport(config.ScramUsername, config.ScramPassword, config.RootCAPath,
			config.InterCAPath, errorLogger)
		if err != nil {
			panic("Secure kafka couldn't connect " + err.Error())
		}
		writer.Transport = transport
	}
	return &segmentio{
		producerBatch: newProducerBatch(config.ProducerBatchTickerDuration, writer, config.ProducerBatchSize, logger, errorLogger),
	}
}

func createSecureKafkaTransport(
	scramUsername,
	scramPassword,
	rootCAPath,
	interCAPath string,
	errorLogger logger.Logger,
) (*kafka.Transport, error) {
	mechanism, err := scram.Mechanism(scram.SHA512, scramUsername, scramPassword)
	if err != nil {
		return nil, err
	}

	caCert, err := os.ReadFile(os.ExpandEnv(rootCAPath))
	if err != nil {
		errorLogger.Printf("An error occurred while reading ca.pem file! Error: %s", err.Error())
		return nil, err
	}

	intCert, err := os.ReadFile(os.ExpandEnv(interCAPath))
	if err != nil {
		errorLogger.Printf("An error occurred while reading int.pem file! Error: %s", err.Error())
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	caCertPool.AppendCertsFromPEM(intCert)

	return &kafka.Transport{
		TLS: &tls.Config{
			RootCAs:    caCertPool,
			MinVersion: tls.VersionTLS12,
		},
		SASL: mechanism,
	}, nil
}

func (a *segmentio) Produce(message []byte, key []byte, headers map[string]string) {
	msg := producer.MessagePool.Get().(*kafka.Message)
	msg.Key = key
	msg.Value = message
	msg.Headers = producer.NewHeaders(headers)
	a.producerBatch.messageChn <- msg
}

func (a *segmentio) Close() error {
	a.producerBatch.isClosed <- true
	// TODO: Wait until batch is clear
	time.Sleep(2 * time.Second)
	return a.producerBatch.Writer.Close()
}
