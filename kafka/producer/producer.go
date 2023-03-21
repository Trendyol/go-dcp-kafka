package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"math"
	"os"
	"time"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/logger"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(ctx *models.ListenerContext, message []byte, key []byte, headers map[string]string, topic string)
	Close() error
}

type producer struct {
	producerBatch *producerBatch
}

func NewProducer(config *config.Kafka, logger logger.Logger, errorLogger logger.Logger, dcpCheckpointCommit func()) (Producer, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Balancer:     &kafka.Hash{},
		BatchSize:    config.ProducerBatchSize,
		BatchBytes:   math.MaxInt,
		BatchTimeout: 500 * time.Microsecond,
		MaxAttempts:  math.MaxInt,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(config.RequiredAcks),
		Logger:       logger,
		ErrorLogger:  errorLogger,
		Compression:  kafka.Compression(config.GetCompression()),
	}

	if config.SecureConnection {
		transport, err := createSecureKafkaTransport(config.ScramUsername, config.ScramPassword, config.RootCAPath,
			config.InterCAPath, errorLogger)
		if err != nil {
			return nil, err
		}
		writer.Transport = transport
	}

	return &producer{
		producerBatch: newProducerBatch(
			config.ProducerBatchTickerDuration,
			writer,
			config.ProducerBatchSize,
			logger,
			errorLogger,
			dcpCheckpointCommit),
	}, nil
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

func (a *producer) Produce(ctx *models.ListenerContext, message []byte, key []byte, headers map[string]string, topic string) {
	a.producerBatch.AddMessage(ctx, message, key, newHeaders(headers), topic)
}

func newHeaders(headersMap map[string]string) []kafka.Header {
	var headers []kafka.Header
	for key, value := range headersMap {
		headers = append(headers, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}
	return headers
}

func (a *producer) Close() error {
	a.producerBatch.Close()
	return a.producerBatch.Writer.Close()
}
