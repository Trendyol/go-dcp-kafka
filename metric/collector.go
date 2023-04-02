package metric

import (
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-kafka-connect-couchbase/kafka/producer"
	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	producer producer.Producer

	dcpLatency *prometheus.Desc
}

func (s *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

func (s *Collector) Collect(ch chan<- prometheus.Metric) {
	producerMetric := s.producer.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		s.dcpLatency,
		prometheus.GaugeValue,
		producerMetric.KafkaConnectorLatency.Value(),
		[]string{}...,
	)
}

func NewMetricCollector(producer producer.Producer) *Collector {
	return &Collector{
		producer: producer,

		dcpLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "kafka_connector_latency_ms", "current"),
			"Kafka connector latency ms at 10sec windows",
			[]string{},
			nil,
		),
	}
}
