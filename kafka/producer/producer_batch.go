package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-kafka-connect-couchbase/logger"

	"github.com/segmentio/kafka-go"
)

type producerBatch struct {
	logger              logger.Logger
	errorLogger         logger.Logger
	batchTicker         *time.Ticker
	Writer              *kafka.Writer
	dcpCheckpointCommit func()
	messages            []kafka.Message
	batchTickerDuration time.Duration
	batchLimit          int
	flushLock           sync.Mutex
}

func newProducerBatch(batchTime time.Duration, writer *kafka.Writer, batchLimit int, logger logger.Logger, errorLogger logger.Logger, dcpCheckpointCommit func()) *producerBatch {
	batch := &producerBatch{
		batchTickerDuration: batchTime,
		batchTicker:         time.NewTicker(batchTime),
		messages:            make([]kafka.Message, 0, batchLimit),
		Writer:              writer,
		batchLimit:          batchLimit,
		logger:              logger,
		errorLogger:         errorLogger,
		dcpCheckpointCommit: dcpCheckpointCommit,
	}
	batch.StartBatchTicker()
	return batch
}

func (b *producerBatch) StartBatchTicker() {
	go func() {
		for {
			<-b.batchTicker.C
			err := b.FlushMessages()
			if err != nil {
				b.errorLogger.Printf("Batch producer flush error %v", err)
			}
		}
	}()
}

func (b *producerBatch) Close() {
	b.batchTicker.Stop()
	err := b.FlushMessages()
	if err != nil {
		b.errorLogger.Printf("Batch producer flush error %v", err)
	}
}

func (b *producerBatch) AddMessage(ctx *models.ListenerContext, message []byte, key []byte, headers []kafka.Header, topic string) {
	b.flushLock.Lock()
	b.messages = append(b.messages, kafka.Message{Key: key, Value: message, Headers: headers, Topic: topic})
	ctx.Ack()
	b.flushLock.Unlock()

	if len(b.messages) == b.batchLimit {
		err := b.FlushMessages()
		if err != nil {
			b.errorLogger.Printf("Batch producer flush error %v", err)
		}
	}
}

func (b *producerBatch) FlushMessages() error {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	if len(b.messages) == 0 {
		return nil
	}
	err := b.Writer.WriteMessages(context.Background(), b.messages...)
	if err != nil {
		return err
	}
	b.dcpCheckpointCommit()

	b.messages = b.messages[:0]
	b.batchTicker.Reset(b.batchTickerDuration)
	return nil
}
