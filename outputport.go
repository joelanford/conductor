package conductor

type outputPort struct {
	streamName   string
	operatorName string
	channel      chan *Tuple
	channel      chan *Tuple

	tuplesLastSent float64
	tuplesSent     float64
}

func newOutputPort(streamName, operatorName string) *outputPort {
	return &outputPort{
		streamName:   streamName,
		operatorName: operatorName,
		channel:      make(chan *Tuple),
	}
}

func (op *outputPort) submit(t *Tuple) {
	op.tuplesSent++
	op.channel <- t
}

func (op *outputPort) tuplesSentDelta() float64 {
	delta := op.tuplesSent - op.tuplesLastSent
	op.tuplesLastSent = op.tuplesSent
	return delta
}
