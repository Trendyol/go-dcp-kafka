package kafka

import (
	"context"
	"fmt"

	"github.com/Trendyol/go-dcp-kafka/config"
	"github.com/Trendyol/go-dcp/logger"
	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
)

type RejectionLogSinkResponseHandler struct {
	Config      config.Kafka
	KafkaClient Client
	Topic       string
}

func (r *RejectionLogSinkResponseHandler) OnInit(ctx *SinkResponseHandlerInitContext) {
	r.Config = ctx.Config
	r.KafkaClient = ctx.KafkaClient
	r.Topic = ctx.Config.RejectionLog.Topic

	err := r.KafkaClient.CheckTopics([]string{r.Topic})
	if err != nil {
		logger.Log.Error("error while rejection topic exist request, err: %v", err)
		panic(err)
	}
}

func (r *RejectionLogSinkResponseHandler) OnSuccess(_ *SinkResponseHandlerContext) {
}

func (r *RejectionLogSinkResponseHandler) OnError(ctx *SinkResponseHandlerContext) {
	rejectionLog := r.buildRejectionLog(ctx)
	if err := r.publishToKafka(ctx, rejectionLog); err != nil {
		logger.Log.Error("failed to publish rejection log, err: %v", err)
		panic(err)
	}
}

func (r *RejectionLogSinkResponseHandler) buildRejectionLog(ctx *SinkResponseHandlerContext) RejectionLog {
	rejectionLog := RejectionLog{
		Topic: ctx.Message.Topic,
		Key:   ctx.Message.Key,
		Error: ctx.Err.Error(),
	}

	if r.Config.RejectionLog.IncludeValue {
		rejectionLog.Value = string(ctx.Message.Value)
	}

	return rejectionLog
}

func (r *RejectionLogSinkResponseHandler) publishToKafka(ctx *SinkResponseHandlerContext, rejectionLog RejectionLog) error {
	writer := r.KafkaClient.Producer()
	defer func() {
		if err := writer.Close(); err != nil {
			logger.Log.Error("failed to close Kafka writer, err: %v", err)
		}
	}()

	logBytes, err := jsoniter.Marshal(rejectionLog)
	if err != nil {
		return fmt.Errorf("failed to marshal rejection log: %w", err)
	}

	kafkaMessage := kafka.Message{
		Topic:   r.Topic,
		Key:     rejectionLog.Key,
		Value:   logBytes,
		Headers: ctx.Message.Headers,
	}

	if err := writer.WriteMessages(context.Background(), kafkaMessage); err != nil {
		return fmt.Errorf("failed to write Kafka message: %w", err)
	}

	return nil
}

func NewRejectionLogSinkResponseHandler() SinkResponseHandler {
	return &RejectionLogSinkResponseHandler{}
}

type RejectionLog struct {
	Topic string
	Key   []byte
	Value string
	Error string
}
