package kafka

type Message struct {
	Key     *string
	Value   interface{}
	Headers map[string]string
}
