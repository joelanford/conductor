package conductor

import "context"

type OperatorContext struct {
	Name            string
	Context         context.Context
	OutputCollector OutputCollector
}

type ProcessFunc func(ctx *OperatorContext, t *Tuple)

type Operator struct {
	name string

	inputStream      string
	inputPartitioner Partitioner
	inputQueueSize   int

	process ProcessFunc

	parallelism int

	outputStream string
}

func NewOperator(name string, process ProcessFunc, parallelism int) *Operator {
	return &Operator{name: name, process: process, parallelism: parallelism}
}

func (o *Operator) Consumes(name string, partitioner Partitioner, queueSize int) *Operator {
	o.inputStream = name
	o.inputPartitioner = partitioner
	o.inputQueueSize = queueSize
	return o
}

func (o *Operator) Produces(name string) *Operator {
	o.outputStream = name
	return o
}

type OutputCollector struct {
	metadata *TupleMetadata
	output   chan<- *Tuple
}

func (o *OutputCollector) Submit(t TupleData) {
	o.output <- &Tuple{Metadata: o.metadata, Data: t}
}
