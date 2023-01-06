package kafka

import (
	"context"
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
	messageChn          chan *kafka.Message
	messages            []kafka.Message
	batchTickerDuration time.Duration
	batchLimit          int
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
		messageChn:          make(chan *kafka.Message, batchLimit),
		messages:            make([]kafka.Message, 0, batchLimit),
		Writer:              writer,
		batchLimit:          batchLimit,
		isClosed:            make(chan bool, 1),
		logger:              logger,
		errorLogger:         errorLogger,
	}
	batch.StartBatch()
	return batch
}

func (b *producerBatch) StartBatch() {
	go func() {
		for {
			select {
			case <-b.isClosed:
				b.batchTicker.Stop()
				err := b.FlushMessages()
				if err != nil {
					b.errorLogger.Printf("Batch producer flush error %v", err)
				}
			case <-b.batchTicker.C:
				err := b.FlushMessages()
				if err != nil {
					b.errorLogger.Printf("Batch producer flush error %v", err)
				}

			case message := <-b.messageChn:
				b.messages = append(b.messages, *message)
				KafkaMessagePool.Put(message)
				if len(b.messages) == b.batchLimit {
					err := b.FlushMessages()
					if err != nil {
						b.errorLogger.Printf("Batch producer flush error %v", err)
					}
				}
			}
		}
	}()
}

func (b *producerBatch) FlushMessages() error {
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
