package streams

import "github.com/prometheus/client_golang/prometheus"

type outputPort struct {
	streamName   string
	operatorName string
	portNum      int
	output       chan *Tuple

	tuplesSent *prometheus.CounterVec
}

func newOutputPort(streamName, operatorName string, portNum int, output chan *Tuple) *outputPort {
	return &outputPort{
		streamName:   streamName,
		operatorName: operatorName,
		portNum:      portNum,
		output:       output,
		tuplesSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "streams",
			Name:      "tuples_sent_total",
			Help:      "The total number of tuples sent by an operator in a streams topology",
		}, []string{"operator", "stream", "port"}),
	}
}
