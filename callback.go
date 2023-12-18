package dcpkafka

import "github.com/Trendyol/go-dcp-kafka/kafka/message"

type callback struct {
}

func (c *callback) OnSuccess(message message.KafkaMessage) {

}

func (c *callback) OnError(message message.KafkaMessage) {

}
