package franz

import (
	"github.com/Trendyol/go-kafka-connect-couchbase/config"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/producer"
	"github.com/Trendyol/go-kafka-connect-couchbase/logger"
	"github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

type franz struct {
	client *kgo.Client
}

func NewProducer(config *config.Kafka, logger logger.Logger, errorLogger logger.Logger) producer.Producer {
	client := createKafkaClient(config, logger)
	return &franz{client: client}
}

func createKafkaClient(config *config.Kafka, l logger.Logger) *kgo.Client {

	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ConsumeTopics(config.Topic),
		kgo.ProducerBatchMaxBytes(int32(config.ProducerBatchSize)),
		//kgo.WithLogger(l), //TODO if we choose franz, we could manager logger interface
		//kgo.DialTLSConfig(), //
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		panic("kafka client failed:" + err.Error())
	}
	return client
}

func (a franz) Produce(message []byte, key []byte, headers map[string]string) {
	msg := producer.MessagePool.Get().(*kafka.Message)
	msg.Key = key
	msg.Value = message
	msg.Headers = producer.NewHeaders(headers)
}

func (a franz) Close() error {
	a.client.Close()
	return nil
}
