package message

type KafkaMessage struct {
	Headers map[string]string
	Key     []byte
	Value   []byte
}
