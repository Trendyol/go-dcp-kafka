package producer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"

	gKafka "github.com/Trendyol/go-dcp-kafka/kafka"
	"github.com/Trendyol/go-dcp-kafka/kafka/message"
	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp/models"
	"github.com/segmentio/kafka-go"
)

type Batch struct {
	sinkResponseHandler gKafka.SinkResponseHandler
	batchTicker         *time.Ticker
	Writer              *kafka.Writer
	dcpCheckpointCommit func()
	metric              *Metric
	messages            []kafka.Message
	currentMessageBytes int64
	batchTickerDuration time.Duration
	batchLimit          int
	batchBytes          int64
	flushLock           sync.Mutex
	isDcpRebalancing    bool
}

func newBatch(
	batchTime time.Duration,
	writer *kafka.Writer,
	batchLimit int,
	batchBytes int64,
	dcpCheckpointCommit func(),
	sinkResponseHandler gKafka.SinkResponseHandler,
) *Batch {
	batch := &Batch{
		batchTickerDuration: batchTime,
		batchTicker:         time.NewTicker(batchTime),
		metric:              &Metric{},
		messages:            make([]kafka.Message, 0, batchLimit),
		Writer:              writer,
		batchLimit:          batchLimit,
		dcpCheckpointCommit: dcpCheckpointCommit,
		batchBytes:          batchBytes,
		sinkResponseHandler: sinkResponseHandler,
	}
	return batch
}

func (b *Batch) StartBatchTicker() {
	go func() {
		for {
			<-b.batchTicker.C
			b.FlushMessages()
		}
	}()
}

func (b *Batch) Close() {
	b.batchTicker.Stop()
	b.FlushMessages()
}

func (b *Batch) PrepareStartRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = true
	b.messages = b.messages[:0]
	b.currentMessageBytes = 0
}

func (b *Batch) PrepareEndRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.isDcpRebalancing = false
}

func (b *Batch) AddMessages(ctx *models.ListenerContext, messages []kafka.Message, eventTime time.Time, isLastChunk bool) {
	b.flushLock.Lock()
	if b.isDcpRebalancing {
		logger.Log.Error("could not add new message to batch while rebalancing")
		b.flushLock.Unlock()
		return
	}
	b.messages = append(b.messages, messages...)
	b.currentMessageBytes += totalSizeOfMessages(messages)
	if isLastChunk {
		ctx.Ack()
	}
	b.flushLock.Unlock()

	if isLastChunk {
		b.metric.KafkaConnectorLatency = time.Since(eventTime).Milliseconds()
	}

	if len(b.messages) >= b.batchLimit || b.currentMessageBytes >= b.batchBytes {
		b.FlushMessages()
	}
}

func (b *Batch) FlushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	if b.isDcpRebalancing {
		return
	}
	if len(b.messages) > 0 {
		startedTime := time.Now()
		err := b.Writer.WriteMessages(context.Background(), b.messages...)

		if err != nil && b.sinkResponseHandler == nil {
			if isFatalError(err) {
				panic(fmt.Errorf("permanent error on Kafka side %v", err))
			}
			logger.Log.Error("batch producer flush error %v", err)
			return
		}

		b.metric.BatchProduceLatency = time.Since(startedTime).Milliseconds()

		if b.sinkResponseHandler != nil {
			switch e := err.(type) {
			case nil:
				b.handleResponseSuccess()
			case kafka.WriteErrors:
				b.handleWriteError(e)
			case kafka.MessageTooLargeError:
				b.handleMessageTooLargeError(e)
				return
			default:
				b.handleResponseError(e)
				logger.Log.Error("batch producer flush error %v", err)
				return
			}
		}
		b.messages = b.messages[:0]
		b.currentMessageBytes = 0
		b.batchTicker.Reset(b.batchTickerDuration)
	}
	b.dcpCheckpointCommit()
}

func isFatalError(err error) bool {
	e, ok := err.(kafka.Error)
	if ok && errors.Is(err, kafka.UnknownTopicOrPartition) {
		return true
	}
	if (ok && e.Temporary()) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) {
		return false
	}
	return true
}

func (b *Batch) handleWriteError(writeErrors kafka.WriteErrors) {
	for i := range writeErrors {
		if writeErrors[i] != nil {
			b.sinkResponseHandler.OnError(&gKafka.SinkResponseHandlerContext{
				Message: convertKafkaMessage(b.messages[i]),
				Err:     writeErrors[i],
			})
		} else {
			b.sinkResponseHandler.OnSuccess(&gKafka.SinkResponseHandlerContext{
				Message: convertKafkaMessage(b.messages[i]),
				Err:     nil,
			})
		}
	}
}

func (b *Batch) handleResponseError(err error) {
	for _, msg := range b.messages {
		b.sinkResponseHandler.OnError(&gKafka.SinkResponseHandlerContext{
			Message: convertKafkaMessage(msg),
			Err:     err,
		})
	}
}

func (b *Batch) handleResponseSuccess() {
	for _, msg := range b.messages {
		b.sinkResponseHandler.OnSuccess(&gKafka.SinkResponseHandlerContext{
			Message: convertKafkaMessage(msg),
			Err:     nil,
		})
	}
}

func (b *Batch) handleMessageTooLargeError(mTooLargeError kafka.MessageTooLargeError) {
	b.sinkResponseHandler.OnError(&gKafka.SinkResponseHandlerContext{
		Message: convertKafkaMessage(mTooLargeError.Message),
		Err:     mTooLargeError,
	})
}

func convertKafkaMessage(src kafka.Message) *message.KafkaMessage {
	return &message.KafkaMessage{
		Topic:   src.Topic,
		Headers: src.Headers,
		Key:     src.Key,
		Value:   src.Value,
	}
}

func totalSizeOfMessages(messages []kafka.Message) int64 {
	var size int
	for _, m := range messages {
		headerSize := 0
		for _, header := range m.Headers {
			headerSize += 2 + len(header.Key)
			headerSize += len(header.Value)
		}
		size += 14 + (4 + len(m.Key)) + (4 + len(m.Value)) + headerSize
	}
	return int64(size)
}
