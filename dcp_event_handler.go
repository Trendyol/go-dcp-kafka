package dcpkafka

import "github.com/Trendyol/go-dcp-kafka/kafka/producer"

type DcpEventHandler struct {
	producerBatch *producer.Batch
}

func (h *DcpEventHandler) BeforeRebalanceStart() {
}

func (h *DcpEventHandler) AfterRebalanceStart() {
}

func (h *DcpEventHandler) BeforeRebalanceEnd() {
}

func (h *DcpEventHandler) AfterRebalanceEnd() {
}

func (h *DcpEventHandler) BeforeStreamStart() {
	h.producerBatch.PrepareEndRebalancing()
}

func (h *DcpEventHandler) AfterStreamStart() {
}

func (h *DcpEventHandler) BeforeStreamStop() {
	h.producerBatch.PrepareStartRebalancing()
}

func (h *DcpEventHandler) AfterStreamStop() {
}
