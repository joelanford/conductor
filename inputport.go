package conductor

type inputPort struct {
	streamName   string
	operatorName string
	input        chan *Tuple
	outputs      []chan *Tuple
	partition    PartitionFunc
}

func newInputPort(streamName, operatorName string, partitionFunc PartitionFunc, parallelism, queueSize int) *inputPort {
	outputs := make([]chan *Tuple, parallelism)
	for i := 0; i < parallelism; i++ {
		outputs[i] = make(chan *Tuple, queueSize)
	}

	var input chan *Tuple
	if parallelism == 1 {
		input = outputs[0]
	} else {
		input = make(chan *Tuple)
	}

	return &inputPort{
		streamName:   streamName,
		operatorName: operatorName,
		input:        input,
		outputs:      outputs,
		partition:    partitionFunc,
	}
}

func (i *inputPort) run() {
	if len(i.outputs) != 1 {
		for t := range i.input {
			i.outputs[i.partition(t)%len(i.outputs)] <- t
		}
		for _, o := range i.outputs {
			close(o)
		}
	}
}
