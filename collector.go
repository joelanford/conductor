package conductor

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type OperatorCollector struct {
	metrics []prometheus.Collector
	mutex   sync.Mutex
}

func NewOperatorCollector() *OperatorCollector {
	return &OperatorCollector{
		metrics: make([]prometheus.Collector, 0),
	}
}

func (oc *OperatorCollector) Describe(ch chan<- *prometheus.Desc) {
	oc.mutex.Lock()
	defer oc.mutex.Unlock()
	for _, m := range oc.metrics {
		m.Describe(ch)
	}
}

func (oc *OperatorCollector) Collect(ch chan<- prometheus.Metric) {
	oc.mutex.Lock()
	defer oc.mutex.Unlock()
	for _, m := range oc.metrics {
		m.Collect(ch)
	}
}

func (oc *OperatorCollector) Register(c prometheus.Collector) {
	oc.mutex.Lock()
	defer oc.mutex.Unlock()
	oc.metrics = append(oc.metrics, c)
}

type TopologyCollector struct {
	topology *Topology
}

func (t *Topology) NewPrometheusCollector() *TopologyCollector {
	return &TopologyCollector{
		topology: t,
	}
}

func (tc *TopologyCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, bolt := range tc.topology.bolts {
		bolt.metricsCollector.Describe(ch)
	}
	for _, spout := range tc.topology.spouts {
		spout.metricsCollector.Describe(ch)
	}
}

func (tc *TopologyCollector) Collect(ch chan<- prometheus.Metric) {
	for _, bolt := range tc.topology.bolts {
		bolt.metricsCollector.Collect(ch)

	}
	for _, spout := range tc.topology.spouts {
		spout.metricsCollector.Collect(ch)

	}
}
