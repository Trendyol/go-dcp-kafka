package main

import (
	"connector/config"
	"connector/couchbase"
	kafka "connector/kafka/producer"
	"context"
	godcpclient "github.com/Trendyol/go-dcp-client"
	"log"
)

type Connector interface {
	Start()
	Close()
}

type connector struct {
	dcp      godcpclient.Dcp
	mapper   Mapper
	producer kafka.Producer
	config   *config.Config
}

func (c *connector) Start() {
	c.dcp.Start()
}

func (c *connector) Close() {
	c.dcp.Close()
}

func (c *connector) listener(event interface{}, err error) {
	if err != nil {
		log.Printf("error | %v", err)
		return
	}

	var e *couchbase.Event
	switch event := event.(type) {
	case godcpclient.DcpMutation:
		v := string(event.Value)
		e = couchbase.NewMutateEvent(string(event.Key), &v)
	case godcpclient.DcpExpiration:
		e = couchbase.NewExpireEvent(string(event.Key), nil)
	case godcpclient.DcpDeletion:
		e = couchbase.NewDeleteEvent(string(event.Key), nil)
	}

	if message := c.mapper(e); message != nil {

		messageValue, err := JsonIter.Marshal(message.Value)
		if err != nil {
			log.Printf("error | %v", err)
			return
		}

		var messageKey []byte
		if message.Key != nil {
			messageKey = []byte(*message.Key)
		} else {
			messageKey = nil
		}

		// TODO: use contexts
		ctx := context.TODO()
		err = c.producer.Produce(&ctx, messageValue, messageKey, message.Headers)
		if err != nil {
			panic("error")
		}
	}

}

func NewConnector(configPath string, mapper Mapper) Connector {
	c := config.NewConfig("cbgokafka", configPath)

	connector := &connector{
		mapper: mapper,
		config: c,
	}

	dcp, err := godcpclient.NewDcp(configPath, connector.listener)
	if err != nil {
		panic(err)
	}
	connector.dcp = dcp
	connector.producer = kafka.NewProducer(c.Kafka)
	return connector
}
