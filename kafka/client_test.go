package kafka

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-kafka/config"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/segmentio/kafka-go"
)

type MockLogger struct {
	debugMessages []string
	errorMessages []string
}

func TestProducer(t *testing.T) {
	// given
	mockLogger := &MockLogger{}
	logger.Log = mockLogger

	mockConfig := config.Connector{
		Kafka: config.Kafka{
			Brokers:                []string{"localhost:9092"},
			ProducerBatchSize:      10,
			ProducerMaxAttempts:    5,
			ReadTimeout:            time.Second,
			WriteTimeout:           time.Second,
			RequiredAcks:           1,
			AllowAutoTopicCreation: true,
		},
	}

	client := &client{config: &mockConfig}

	// when
	writer := client.Producer()

	// then
	t.Run("Completion callback should log success message", func(t *testing.T) {
		messages := []kafka.Message{
			{WriterData: "message 1"},
		}

		writer.Completion(messages, nil)

		expected := "message produce success, message: %s\n"
		if !contains(mockLogger.debugMessages, expected) {
			t.Errorf("Expected log message: %s, but got: %v", expected, mockLogger.debugMessages)
		}
	})

	t.Run("Completion callback should log error message", func(t *testing.T) {
		messages := []kafka.Message{
			{WriterData: "message 2"},
		}

		writer.Completion(messages, errors.New("Kafka error"))

		expected := "message produce error, error: %s message: %s, \n"
		if !contains(mockLogger.errorMessages, expected) {
			t.Errorf("Expected log message: %s, but got: %v", expected, mockLogger.errorMessages)
		}
	})
}

func contains(list []string, item string) bool {
	for _, v := range list {
		if reflect.DeepEqual(v, item) {
			return true
		}
	}
	return false
}

func (m *MockLogger) Trace(message string, args ...interface{}) { panic("implement me") }

func (m *MockLogger) Info(message string, args ...interface{}) { panic("implement me") }

func (m *MockLogger) Warn(message string, args ...interface{}) { panic("implement me") }

func (m *MockLogger) Log(level string, message string, args ...interface{}) { panic("implement me") }

func (m *MockLogger) Debug(format string, args ...interface{}) {
	m.debugMessages = append(m.debugMessages, format)
}

func (m *MockLogger) Error(format string, args ...interface{}) {
	m.errorMessages = append(m.errorMessages, format)
}
