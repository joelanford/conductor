package conductor

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

type OutputPort struct {
	streamName   string
	operatorName string
	portNum      int
	output       chan *Tuple

	tuplesSent *prometheus.CounterVec
}

func NewOutputPort(streamName, operatorName string, portNum int, output chan *Tuple) *OutputPort {
	return &OutputPort{
		streamName:   streamName,
		operatorName: operatorName,
		portNum:      portNum,
		output:       output,
		tuplesSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "conductor",
			Name:      "tuples_sent_total",
			Help:      "The total number of tuples sent by an operator in a conductor topology",
		}, []string{"operator", "stream", "port"}),
	}
}

func (op *OutputPort) StreamName() string {
	return op.streamName
}

func (op *OutputPort) OperatorName() string {
	return op.operatorName
}

func (op *OutputPort) Submit(t *Tuple) {
	op.tuplesSent.WithLabelValues(op.operatorName, op.streamName, strconv.Itoa(op.portNum)).Inc()
	op.output <- t
}

func (op *OutputPort) Close() {
	close(op.output)
}
