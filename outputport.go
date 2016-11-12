package conductor

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

type outputPort struct {
	streamName   string
	operatorName string
	portNum      int
	channel      chan *Tuple

	tuplesSent *prometheus.CounterVec
}

func newOutputPort(streamName, operatorName string, portNum int) *outputPort {
	return &outputPort{
		streamName:   streamName,
		operatorName: operatorName,
		portNum:      portNum,
		channel:      make(chan *Tuple),
		tuplesSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "conductor",
			Name:      "tuples_sent_total",
			Help:      "The total number of tuples sent by an operator in a conductor topology",
		}, []string{"operator", "stream", "port"}),
	}
}

func (op *outputPort) submit(t *Tuple) {
	op.tuplesSent.WithLabelValues(op.operatorName, op.streamName, strconv.Itoa(op.portNum)).Inc()
	op.channel <- t
}
