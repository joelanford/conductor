package streams

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type MetricsCollector struct {
	metrics []prometheus.Collector
	mutex   sync.Mutex
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics: make([]prometheus.Collector, 0),
	}
}

func (mc *MetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	for _, m := range mc.metrics {
		m.Describe(ch)
	}
}

func (mc *MetricsCollector) Collect(ch chan<- prometheus.Metric) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	for _, m := range mc.metrics {
		m.Collect(ch)
	}
}

func (mc *MetricsCollector) Register(c prometheus.Collector) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()
	mc.metrics = append(mc.metrics, c)
}

type TopologyCollector struct {
	topology *Topology
}

func (t *Topology) Collector() *TopologyCollector {
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
