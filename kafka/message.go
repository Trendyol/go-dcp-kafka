package kafka

type Message struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
}
