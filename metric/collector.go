package metric

import (
	"github.com/Trendyol/go-dcp-kafka/kafka/producer"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	producer producer.Producer

	kafkaConnectorLatency *prometheus.Desc
	batchProduceLatency   *prometheus.Desc
}

func (s *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}

func (s *Collector) Collect(ch chan<- prometheus.Metric) {
	producerMetric := s.producer.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		s.kafkaConnectorLatency,
		prometheus.GaugeValue,
		float64(producerMetric.KafkaConnectorLatency),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		s.batchProduceLatency,
		prometheus.GaugeValue,
		float64(producerMetric.BatchProduceLatency),
		[]string{}...,
	)
}

func NewMetricCollector(producer producer.Producer) *Collector {
	return &Collector{
		producer: producer,

		kafkaConnectorLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "kafka_connector_latency_ms", "current"),
			"Kafka connector latency ms at 10sec windows",
			[]string{},
			nil,
		),

		batchProduceLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "kafka_connector_batch_produce_latency_ms", "current"),
			"Kafka connector batch produce latency ms",
			[]string{},
			nil,
		),
	}
}
