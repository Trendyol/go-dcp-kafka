package metadata

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/Trendyol/go-dcp/metadata"
	"github.com/Trendyol/go-dcp/models"
	"github.com/Trendyol/go-dcp/wrapper"

	gKafka "github.com/Trendyol/go-dcp-kafka/kafka"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
)

type connectMetadata struct {
	kafkaClient gKafka.Client
	writer      *kafka.Writer
	topic       string
	groupName   string
	bucketName  string
}

type keyElementData struct {
	Bucket    string `json:"bucket"`
	Partition string `json:"partition"`
}

type keyElement struct {
	DataVal  keyElementData
	StrVal   string
	IsString bool
}

func (k keyElement) MarshalJSON() ([]byte, error) {
	if k.IsString {
		return jsoniter.Marshal(k.StrVal)
	}
	return jsoniter.Marshal(k.DataVal)
}

func (k *keyElement) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	switch data[0] {
	case '"':
		k.IsString = true
		return jsoniter.Unmarshal(data, &k.StrVal)
	case '{':
		k.IsString = false
		return jsoniter.Unmarshal(data, &k.DataVal)
	default:
		return fmt.Errorf("unexpected JSON token for KeyElement: %s", string(data[0]))
	}
}

type valueElement struct {
	VBUuid                 uint64 `json:"vbuuid"`
	CollectionsManifestUid int    `json:"collectionsManifestUid"`
	SnapshotEndSeqNo       uint64 `json:"snapshotEndSeqno"`
	BySeqNo                uint64 `json:"bySeqno"`
	SnapshotStartSeqNo     uint64 `json:"snapshotStartSeqno"`
}

func (s *connectMetadata) Save(state map[uint16]*models.CheckpointDocument, dirtyOffsets map[uint16]bool, _ string) error {
	messages := make([]kafka.Message, 0, len(state))
	for vbID, document := range state {
		if !dirtyOffsets[vbID] {
			continue
		}

		value, err := jsoniter.Marshal(&valueElement{
			VBUuid:                 document.Checkpoint.VbUUID,
			CollectionsManifestUid: 1,
			SnapshotEndSeqNo:       document.Checkpoint.Snapshot.EndSeqNo,
			BySeqNo:                document.Checkpoint.SeqNo,
			SnapshotStartSeqNo:     document.Checkpoint.Snapshot.StartSeqNo,
		})
		if err != nil {
			return err
		}

		key, err := jsoniter.Marshal([]keyElement{
			{
				IsString: true,
				StrVal:   s.groupName,
			},
			{
				DataVal: keyElementData{
					Bucket:    s.bucketName,
					Partition: strconv.Itoa(int(vbID)),
				},
			},
		})
		if err != nil {
			return err
		}

		messages = append(messages, kafka.Message{
			Topic: s.topic,
			Key:   key,
			Value: value,
		})
	}

	return s.writer.WriteMessages(context.Background(), messages...)
}

func (s *connectMetadata) Load( //nolint:funlen
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
			var key []keyElement
			var value valueElement
			var doc *models.CheckpointDocument
			var vbID uint16

			err := jsoniter.Unmarshal(m.Key, &key)
			if err != nil {
				logger.Log.Warn("connect metadata key unmarshall error: %v", err)
			}

			err = jsoniter.Unmarshal(m.Value, &value)
			if err != nil {
				logger.Log.Warn("connect metadata value unmarshall error: %v", err)
			}

			if len(key)%2 != 0 {
				logger.Log.Warn("connect metadata key does not have an even number of elements")
			}

			for i := 0; i < len(key); i += 2 {
				if !key[i].IsString {
					logger.Log.Warn("connect metadata key index = %d need to be string", i)
					continue
				}

				if key[i+1].IsString {
					logger.Log.Warn("connect metadata key index = %d does not need to be string", i)
					continue
				}

				if key[i].StrVal == s.groupName {
					var errParse error
					vbID64, errParse := strconv.ParseUint(key[i+1].DataVal.Partition, 10, 16)
					if errParse != nil {
						logger.Log.Error("connect metadata key partition parse error: %v", errParse)
						panic(errParse)
					}
					vbID = uint16(vbID64)
					doc = NewCheckpointDocumentFromValueElement(&value, bucketUUID)
					break
				}
			}

			if doc == nil {
				doc = models.NewEmptyCheckpointDocument(bucketUUID)
			} else {
				exist = true
			}

			state.Store(vbID, doc)
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

func (s *connectMetadata) Clear(_ []uint16) error {
	return nil
}

func NewCheckpointDocumentFromValueElement(el *valueElement, bucketUUID string) *models.CheckpointDocument {
	return &models.CheckpointDocument{
		Checkpoint: &models.CheckpointDocumentCheckpoint{
			VbUUID: el.VBUuid,
			SeqNo:  el.BySeqNo,
			Snapshot: &models.CheckpointDocumentSnapshot{
				StartSeqNo: el.SnapshotStartSeqNo,
				EndSeqNo:   el.SnapshotEndSeqNo,
			},
		},
		BucketUUID: bucketUUID,
	}
}

func NewConnectMetadata(
	kafkaClient gKafka.Client,
	groupName string,
	bucketName string,
) metadata.Metadata {
	return &connectMetadata{
		kafkaClient: kafkaClient,
		writer:      kafkaClient.Producer(nil),
		topic:       "connect-offsets",
		groupName:   groupName,
		bucketName:  bucketName,
	}
}
