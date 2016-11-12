package conductor

import "github.com/prometheus/client_golang/prometheus"

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
		bolt.tuplesReceived.Describe(ch)

		for _, op := range bolt.outputs {
			op.tuplesSent.Describe(ch)
		}
	}
	for _, spout := range tc.topology.spouts {
		for _, op := range spout.outputs {
			op.tuplesSent.Describe(ch)
		}
	}
}

func (tc *TopologyCollector) Collect(ch chan<- prometheus.Metric) {
	for _, bolt := range tc.topology.bolts {
		bolt.tuplesReceived.Collect(ch)

		for _, op := range bolt.outputs {
			op.tuplesSent.Collect(ch)
		}
	}
	for _, spout := range tc.topology.spouts {
		for _, op := range spout.outputs {
			op.tuplesSent.Collect(ch)
		}
	}
}
