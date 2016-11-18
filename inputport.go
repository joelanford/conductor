package conductor

type InputPort struct {
	streamName   string
	operatorName string
	input        chan *Tuple
	outputs      []chan *Tuple
	partition    PartitionFunc
}

func NewInputPort(streamName, operatorName string, partitionFunc PartitionFunc, parallelism int, input chan *Tuple) *InputPort {
	outputs := make([]chan *Tuple, parallelism)
	if parallelism == 1 {
		outputs[0] = input
	} else {
		for i := 0; i < parallelism; i++ {
			outputs[i] = make(chan *Tuple)
		}
	}

	return &InputPort{
		streamName:   streamName,
		operatorName: operatorName,
		input:        input,
		outputs:      outputs,
		partition:    partitionFunc,
	}
}

func (i *InputPort) Run() {
	if len(i.outputs) != 1 {
		for t := range i.input {
			i.outputs[i.partition(t)%len(i.outputs)] <- t
		}
		for _, o := range i.outputs {
			close(o)
		}
	}
}

func (i *InputPort) Input() chan<- *Tuple {
	return i.input
}

func (i *InputPort) Output(output int) <-chan *Tuple {
	return i.outputs[output]
}

func (i *InputPort) NumOutputs() int {
	return len(i.outputs)
}

func (i *InputPort) Close() {
	close(i.input)
}

func (i *InputPort) OperatorName() string {
	return i.operatorName
}

func (i *InputPort) StreamName() string {
	return i.streamName
}
