package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/Trendyol/go-kafka-connect-couchbase/logger"

	"github.com/segmentio/kafka-go"
)

type producerBatch struct {
	logger              logger.Logger
	errorLogger         logger.Logger
	batchTicker         *time.Ticker
	Writer              *kafka.Writer
	isClosed            chan bool
	messages            []kafka.Message
	batchTickerDuration time.Duration
	batchLimit          int
	flushMutex          sync.Mutex
}

func newProducerBatch(
	batchTime time.Duration,
	writer *kafka.Writer,
	batchLimit int,
	logger logger.Logger,
	errorLogger logger.Logger,
) *producerBatch {
	batch := &producerBatch{
		batchTickerDuration: batchTime,
		batchTicker:         time.NewTicker(batchTime),
		messages:            make([]kafka.Message, 0, batchLimit),
		Writer:              writer,
		batchLimit:          batchLimit,
		isClosed:            make(chan bool, 1),
		logger:              logger,
		errorLogger:         errorLogger,
	}
	go func() {
		errChan := make(chan error, 1)
		batch.CheckBatchTicker(errChan)

		for err := range errChan {
			errorLogger.Printf("Batch producer flush error %v", err)
		}
	}()
	return batch
}

func (b *producerBatch) CheckBatchTicker(errChan chan error) {
	for {
		select {
		case <-b.isClosed:
			b.batchTicker.Stop()
			err := b.FlushMessages()
			if err != nil {
				errChan <- err
			}
		case <-b.batchTicker.C:
			err := b.FlushMessages()
			if err != nil {
				errChan <- err
			}
		}
	}
}

func (b *producerBatch) AddMessage(message kafka.Message) error {
	b.messages = append(b.messages, message)
	if len(b.messages) >= b.batchLimit {
		err := b.FlushMessages()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *producerBatch) FlushMessages() error {
	b.flushMutex.Lock()
	defer b.flushMutex.Unlock()

	messageCount := len(b.messages)
	if messageCount == 0 {
		return nil
	}
	err := b.Writer.WriteMessages(context.Background(), b.messages...)
	if err != nil {
		return err
	}

	b.messages = b.messages[:0]
	b.batchTicker.Reset(b.batchTickerDuration)
	return nil
}
