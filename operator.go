package conductor

import "context"

type OperatorContext struct {
	name            string
	outputCollector *OutputCollector
}

func (o *OperatorContext) Name() string {
	return o.name
}

func (o *OperatorContext) OutputCollector() *OutputCollector {
	return o.outputCollector
}

type ProcessFunc func(ctx context.Context, opCtx OperatorContext, instance int)
type ProcessTupleFunc func(ctx context.Context, opCtx OperatorContext, tuple Tuple, instance int)

type OutputCollector struct {
	metadata []*TupleMetadata
	outputs  []chan *Tuple
}

func (o *OutputCollector) Submit(t TupleData, port int) {
	o.outputs[port] <- &Tuple{Metadata: o.metadata[port], Data: t}
}

func (o *OutputCollector) NumPorts() int {
	return len(o.outputs)
}
