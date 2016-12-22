package streams

type inputPort struct {
	streamName   string
	operatorName string
	input        chan *Tuple
	outputs      []chan *Tuple
	partition    PartitionFunc
}

func newInputPort(streamName, operatorName string, partitionFunc PartitionFunc, parallelism int, input chan *Tuple) *inputPort {
	outputs := make([]chan *Tuple, parallelism)
	if parallelism == 1 {
		outputs[0] = input
	} else {
		for i := 0; i < parallelism; i++ {
			outputs[i] = make(chan *Tuple)
		}
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
