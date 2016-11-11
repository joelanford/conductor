package conductor

import "github.com/prometheus/client_golang/prometheus"

type TopologyCollector struct {
	topology *Topology

	spoutTuplesSent      *prometheus.CounterVec
	boltTuplesReceived   *prometheus.CounterVec
	boltTuplesSent       *prometheus.CounterVec
	streamTuplesReceived *prometheus.CounterVec
	streamTuplesSent     *prometheus.CounterVec
}

func NewTopologyCollector(t *Topology) *TopologyCollector {
	return &TopologyCollector{
		topology: t,

		spoutTuplesSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "conductor",
			Name:      "spout_tuples_sent_total",
			Help:      "The total number of tuples sent by a spout in a conductor topology",
		}, []string{"spout"}),

		boltTuplesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "conductor",
			Name:      "bolt_tuples_received_total",
			Help:      "The total number of tuples recevied by a bolt in a conductor topology",
		}, []string{"bolt"}),

		boltTuplesSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "conductor",
			Name:      "bolt_tuples_sent_total",
			Help:      "The total number of tuples sent by a bolt in a conductor topology",
		}, []string{"bolt"}),

		streamTuplesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "conductor",
			Name:      "stream_tuples_received_total",
			Help:      "The total number of tuples received by a stream in a conductor topology",
		}, []string{"stream"}),

		streamTuplesSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "conductor",
			Name:      "stream_tuples_sent_total",
			Help:      "The total number of tuples sent by a stream in a conductor topology",
		}, []string{"stream"}),
	}
}

func (tc *TopologyCollector) Describe(ch chan<- *prometheus.Desc) {
	tc.spoutTuplesSent.Describe(ch)
	tc.boltTuplesReceived.Describe(ch)
	tc.boltTuplesSent.Describe(ch)
	tc.streamTuplesReceived.Describe(ch)
	tc.streamTuplesSent.Describe(ch)
}

func (tc *TopologyCollector) Collect(ch chan<- prometheus.Metric) {
	for _, stream := range tc.topology.streams {
		tc.streamTuplesReceived.WithLabelValues(stream.name).Add(stream.tuplesReceivedDelta())
		tc.streamTuplesSent.WithLabelValues(stream.name).Add(stream.tuplesSentDelta())
	}
	for _, bolt := range tc.topology.bolts {
		for portNum, _ := range bolt.inputs {
			tc.boltTuplesReceived.WithLabelValues(bolt.name).Add(bolt.tuplesReceivedDelta(portNum))
		}
		for portNum, op := range bolt.outputs {
			_ = portNum
			tc.boltTuplesSent.WithLabelValues(bolt.name).Add(op.tuplesSentDelta())
		}
	}
	for _, spout := range tc.topology.spouts {
		for portNum, op := range spout.outputs {
			_ = portNum
			tc.spoutTuplesSent.WithLabelValues(spout.name).Add(op.tuplesSentDelta())
		}
	}

	tc.spoutTuplesSent.Collect(ch)
	tc.boltTuplesReceived.Collect(ch)
	tc.boltTuplesSent.Collect(ch)
	tc.streamTuplesReceived.Collect(ch)
	tc.streamTuplesSent.Collect(ch)
}
