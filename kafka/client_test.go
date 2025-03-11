package kafka

import (
	"errors"
	"fmt"
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

	completionHandler := func(messages []kafka.Message, err error) {
		if err != nil {
			logger.Log.Error("message produce failed: %v", err)
			return
		}
		for _, msg := range messages {
			logger.Log.Debug("message produce success, message: %s", msg.WriterData)
		}
	}

	writer := client.Producer(completionHandler)

	t.Run("Completion callback should log success message", func(t *testing.T) {
		messages := []kafka.Message{
			{WriterData: "message 1"},
		}

		writer.Completion(messages, nil)

		expected := "message produce success, message: message 1"
		if !contains(mockLogger.debugMessages, expected) {
			t.Errorf("Expected log message: %s, but got: %v", expected, mockLogger.debugMessages)
		}
	})

	t.Run("Completion callback should log error message", func(t *testing.T) {
		writer.Completion(nil, errors.New("some error occurred"))

		expected := "message produce failed: some error occurred"
		if !contains(mockLogger.errorMessages, expected) {
			t.Errorf("Expected error log message: %s, but got: %v", expected, mockLogger.errorMessages)
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

func (m *MockLogger) Trace(message string, args ...interface{})             { panic("implement me") }
func (m *MockLogger) Info(message string, args ...interface{})              { panic("implement me") }
func (m *MockLogger) Warn(message string, args ...interface{})              { panic("implement me") }
func (m *MockLogger) Log(level string, message string, args ...interface{}) { panic("implement me") }
func (m *MockLogger) Debug(format string, args ...interface{}) {
	m.debugMessages = append(m.debugMessages, fmt.Sprintf(format, args...))
}
func (m *MockLogger) Error(format string, args ...interface{}) {
	m.errorMessages = append(m.errorMessages, fmt.Sprintf(format, args...))
}
