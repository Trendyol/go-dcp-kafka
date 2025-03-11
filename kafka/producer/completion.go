package producer

import (
	"github.com/Trendyol/go-dcp/logger"
	"github.com/segmentio/kafka-go"
)

func DefaultCompletion(messages []kafka.Message, err error) {
	for i := range messages {
		if err != nil {
			logger.Log.Error("message produce error, error: %s message: %s, \n", err.Error(), messages[i].WriterData)
		} else {
			logger.Log.Debug("message produce success, message: %s\n", messages[i].WriterData)
		}
	}
}
