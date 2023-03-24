package metadata

import (
	"context"
	"errors"
	godcpclient "github.com/Trendyol/go-dcp-client"
	gKafka "github.com/Trendyol/go-kafka-connect-couchbase/kafka"
	"github.com/Trendyol/go-kafka-connect-couchbase/logger"
	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
	"strconv"
	"sync"
)

type kafkaMetadata struct {
	kafkaClient gKafka.Client
	writer      *kafka.Writer
	topic       string
	logger      logger.Logger
	errorLogger logger.Logger
}

func (s *kafkaMetadata) Save(state map[uint16]*godcpclient.CheckpointDocument, dirtyOffsets map[uint16]bool, _ string) error {
	var messages []kafka.Message

	for vbID, document := range state {
		if !dirtyOffsets[vbID] {
			continue
		}

		value, err := jsoniter.Marshal(document)

		if err != nil {
			return err
		}

		messages = append(messages, kafka.Message{
			Topic: s.topic,
			Key:   []byte(strconv.Itoa(int(vbID))),
			Value: value,
		})
	}

	return s.writer.WriteMessages(context.Background(), messages...)
}

func (s *kafkaMetadata) Load(vbIDs []uint16, bucketUUID string) (map[uint16]*godcpclient.CheckpointDocument, bool, error) {
	partitions, err := s.kafkaClient.GetPartitions(s.topic)

	if err != nil {
		return nil, false, err
	}

	endOffsets, err := s.kafkaClient.GetEndOffsets(s.topic, partitions)

	if err != nil {
		return nil, false, err
	}

	ch := make(chan kafka.Message)
	wg := &sync.WaitGroup{}
	wg.Add(len(endOffsets))

	for _, endOffset := range endOffsets {
		consumer := s.kafkaClient.Consumer(s.topic, endOffset.Partition, endOffset.FirstOffset)

		if endOffset.FirstOffset == -1 && endOffset.LastOffset == 0 {
			wg.Done()
			continue
		}

		go func(consumer *kafka.Reader, lastOffset int64) {
			for {
				m, err := consumer.ReadMessage(context.Background())
				if err != nil {
					break
				}

				ch <- m

				if m.Offset+1 >= lastOffset {
					break
				}
			}

			if err := consumer.Close(); err != nil {
				s.logger.Printf("failed to close consumer %v", err)
			}

			wg.Done()
		}(consumer, endOffset.LastOffset)
	}

	state := map[uint16]*godcpclient.CheckpointDocument{}
	stateLock := &sync.Mutex{}
	exist := false

	go func() {
		for m := range ch {
			var doc *godcpclient.CheckpointDocument

			err = jsoniter.Unmarshal(m.Value, &doc)

			if err != nil {
				doc = godcpclient.NewEmptyCheckpointDocument(bucketUUID)
			} else {
				exist = true
			}

			vbID, err := strconv.ParseUint(string(m.Key), 0, 16)

			if err == nil {
				stateLock.Lock()
				state[uint16(vbID)] = doc
				stateLock.Unlock()
			} else {
				s.logger.Printf("cannot load checkpoint, vbID: %d %v", vbID, err)
				panic(err)
			}
		}
	}()

	wg.Wait()

	for _, vbID := range vbIDs {
		if _, ok := state[vbID]; !ok {
			state[vbID] = godcpclient.NewEmptyCheckpointDocument(bucketUUID)
		}
	}

	return state, exist, nil
}

func (s *kafkaMetadata) Clear(_ []uint16) error {
	return nil
}

func NewKafkaMetadata(kafkaClient gKafka.Client, kafkaMetadataConfig map[string]string, logger logger.Logger, errorLogger logger.Logger) godcpclient.Metadata {
	var topic string
	var partition int
	var replicationFactor int

	if _, ok := kafkaMetadataConfig["topic"]; ok {
		topic = kafkaMetadataConfig["topic"]
	} else {
		panic(errors.New("topic is not defined"))
	}

	if topic == "" {
		panic(errors.New("topic is empty"))
	}

	if _, ok := kafkaMetadataConfig["partition"]; ok {
		partition, _ = strconv.Atoi(kafkaMetadataConfig["partition"])
	} else {
		partition = 25
	}

	if partition == 0 {
		panic(errors.New("partition is 0"))
	}

	if _, ok := kafkaMetadataConfig["replicationFactor"]; ok {
		replicationFactor, _ = strconv.Atoi(kafkaMetadataConfig["replicationFactor"])
	} else {
		replicationFactor = 3
	}

	if replicationFactor == 0 {
		panic(errors.New("replicationFactor is 0"))
	}

	metadata := &kafkaMetadata{
		kafkaClient: kafkaClient,
		writer:      kafkaClient.Producer(),
		topic:       topic,
		logger:      logger,
		errorLogger: errorLogger,
	}

	err := kafkaClient.CreateCompactedTopic(topic, partition, replicationFactor)

	if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		panic(err)
	}

	err = kafkaClient.CheckTopicIsCompacted(metadata.topic)
	if err != nil {
		panic(err)
	}

	return metadata
}
