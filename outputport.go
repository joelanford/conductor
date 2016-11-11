package conductor

type outputPort struct {
	streamName   string
	operatorName string
	channel      chan *Tuple
}

func newOutputPort(streamName, operatorName string) *outputPort {
	return &outputPort{
		streamName:   streamName,
		operatorName: operatorName,
		channel:      make(chan *Tuple),
	}
}
