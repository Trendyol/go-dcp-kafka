package kafka

import (
	"errors"
	"github.com/Trendyol/go-dcp-kafka/config"
	"github.com/Trendyol/go-dcp/logger"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
)

type MockLogger struct {
	mock.Mock
}

func TestProducer(t *testing.T) {
	// given
	mockLogger := new(MockLogger)
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
		mockLogger.On("Debug", mock.Anything, mock.Anything).Return()

		messages := []kafka.Message{
			{WriterData: "message 1"},
		}

		writer.Completion(messages, nil)

		mockLogger.AssertCalled(t, "Debug", mock.Anything, mock.Anything)
	})

	t.Run("Completion callback should log error message", func(t *testing.T) {
		mockLogger.On("Error", mock.Anything, mock.Anything).Return()

		messages := []kafka.Message{
			{WriterData: "message 2"},
		}

		writer.Completion(messages, errors.New("Kafka error"))

		mockLogger.AssertCalled(t, "Error", mock.Anything, mock.Anything)
	})
}

func (m *MockLogger) Trace(message string, args ...interface{}) { panic("implement me") }

func (m *MockLogger) Info(message string, args ...interface{}) { panic("implement me") }

func (m *MockLogger) Warn(format string, args ...interface{}) { m.Called(format, args) }

func (m *MockLogger) Log(level string, message string, args ...interface{}) { panic("implement me") }

func (m *MockLogger) Error(format string, args ...interface{}) { m.Called(format, args) }

func (m *MockLogger) Debug(format string, args ...interface{}) { m.Called(format, args) }
