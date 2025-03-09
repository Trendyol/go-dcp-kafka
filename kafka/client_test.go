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

	writer := client.Producer()

	t.Run("Completion callback should log success message with default callback", func(t *testing.T) {
		messages := []kafka.Message{
			{WriterData: "message 1"},
		}

		client.config.Kafka.Completion = config.CompletionType{
			Default: true,
		}

		writer.Completion(messages, nil)

		expected := "message produce success, message: %s\n"
		if !contains(mockLogger.debugMessages, expected) {
			t.Errorf("Expected log message: %s, but got: %v", expected, mockLogger.debugMessages)
		}
	})

	t.Run("Completion callback should log error message with default callback", func(t *testing.T) {
		messages := []kafka.Message{
			{WriterData: "message 2"},
		}

		client.config.Kafka.Completion = config.CompletionType{
			Default: true,
		}

		writer.Completion(messages, errors.New("Kafka error"))

		expected := "message produce error, error: %s message: %s, \n"
		if !contains(mockLogger.errorMessages, expected) {
			t.Errorf("Expected log message: %s, but got: %v", expected, mockLogger.errorMessages)
		}
	})

	client.config.Kafka.Completion = config.CompletionType{
		Func: func(messages []kafka.Message, err error) {
			for i := range messages {
				if err != nil {
					logger.Log.Error("custom error: %s message: %s, \n", err.Error(), messages[i].WriterData)
				} else {
					logger.Log.Debug("custom success: message: %s\n", messages[i].WriterData)
				}
			}
		},
	}

	t.Run("Completion callback should log custom success message", func(t *testing.T) {
		messages := []kafka.Message{
			{WriterData: "message 3"},
		}

		writer.Completion(messages, nil)

		expected := "custom success: message: %s\n"
		if !contains(mockLogger.debugMessages, expected) {
			t.Errorf("Expected log message: %s, but got: %v", expected, mockLogger.debugMessages)
		}
	})

	t.Run("Completion callback should log custom error message", func(t *testing.T) {
		messages := []kafka.Message{
			{WriterData: "message 4"},
		}

		writer.Completion(messages, errors.New("Kafka error"))

		expected := "custom error: %s message: %s, \n"
		if !contains(mockLogger.errorMessages, expected) {
			t.Errorf("Expected log message: %s, but got: %v", expected, mockLogger.errorMessages)
		}
	})

	client.config.Kafka.Completion = config.CompletionType{
		Func: nil,
	}

	t.Run("Completion callback should do nothing when Func is nil", func(t *testing.T) {
		messages := []kafka.Message{
			{WriterData: "message 5"},
		}

		mockLogger.debugMessages = nil
		mockLogger.errorMessages = nil

		writer.Completion(messages, nil)

		if len(mockLogger.debugMessages) > 0 || len(mockLogger.errorMessages) > 0 {
			t.Errorf("Expected no log messages, but got debug: %v, error: %v", mockLogger.debugMessages, mockLogger.errorMessages)
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
