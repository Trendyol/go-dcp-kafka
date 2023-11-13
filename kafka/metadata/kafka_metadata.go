package metadata

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/Trendyol/go-dcp/wrapper"

	"github.com/Trendyol/go-dcp/metadata"
	"github.com/Trendyol/go-dcp/models"

	gKafka "github.com/Trendyol/go-dcp-kafka/kafka"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
)

type kafkaMetadata struct {
	kafkaClient gKafka.Client
	writer      *kafka.Writer
	topic       string
}

func (s *kafkaMetadata) Save(state map[uint16]*models.CheckpointDocument, dirtyOffsets map[uint16]bool, _ string) error {
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

func (s *kafkaMetadata) Load( //nolint:funlen
	vbIDs []uint16,
	bucketUUID string,
) (*wrapper.ConcurrentSwissMap[uint16, *models.CheckpointDocument], bool, error) {
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
				logger.Log.Error("failed to close consumer %v", err)
			}

			wg.Done()
		}(consumer, endOffset.LastOffset)
	}

	state := wrapper.CreateConcurrentSwissMap[uint16, *models.CheckpointDocument](1024)
	exist := false

	go func() {
		for m := range ch {
			var doc *models.CheckpointDocument

			err = jsoniter.Unmarshal(m.Value, &doc)
			if err != nil {
				doc = models.NewEmptyCheckpointDocument(bucketUUID)
			} else {
				exist = true
			}

			vbID, err := strconv.ParseUint(string(m.Key), 0, 16)
			if err == nil {
				state.Store(uint16(vbID), doc)
			} else {
				logger.Log.Error("cannot load checkpoint, vbID: %d %v", vbID, err)
				panic(err)
			}
		}
	}()

	wg.Wait()

	for _, vbID := range vbIDs {
		_, ok := state.Load(vbID)
		if !ok {
			state.Store(vbID, models.NewEmptyCheckpointDocument(bucketUUID))
		}
	}

	return state, exist, nil
}

func (s *kafkaMetadata) Clear(_ []uint16) error {
	return nil
}

func NewKafkaMetadata(
	kafkaClient gKafka.Client,
	kafkaMetadataConfig map[string]any,
) metadata.Metadata {
	var topic string
	var partition int
	var replicationFactor int

	if _, ok := kafkaMetadataConfig["topic"]; ok {
		topic = kafkaMetadataConfig["topic"].(string)
	} else {
		panic(errors.New("topic is not defined"))
	}

	if topic == "" {
		panic(errors.New("topic is empty"))
	}

	if _, ok := kafkaMetadataConfig["partition"]; ok {
		partition, _ = strconv.Atoi(kafkaMetadataConfig["partition"].(string))
	} else {
		partition = 25
	}

	if partition == 0 {
		panic(errors.New("partition is 0"))
	}

	if _, ok := kafkaMetadataConfig["replicationFactor"]; ok {
		replicationFactor, _ = strconv.Atoi(kafkaMetadataConfig["replicationFactor"].(string))
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
