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
