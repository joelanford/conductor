package conductor

import (
	"context"
	"sync"
)

type Topology struct {
	name                string
	sourceOperatorSpecs []*SourceOperatorSpec
	operatorSpecs       []*OperatorSpec
}

func NewTopology(name string) *Topology {
	return &Topology{name: name}
}

func (t *Topology) AddSourceOperator(name string, process ProcessFunc, parallelism int) *SourceOperatorSpec {
	o := &SourceOperatorSpec{
		name:        name,
		process:     process,
		parallelism: parallelism,
	}
	t.sourceOperatorSpecs = append(t.sourceOperatorSpecs, o)
	return o
}

func (t *Topology) AddOperator(name string, process ProcessTupleFunc, parallelism int) *OperatorSpec {
	o := &OperatorSpec{
		name:        name,
		process:     process,
		parallelism: parallelism,
	}
	t.operatorSpecs = append(t.operatorSpecs, o)
	return o
}

func (t *Topology) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	streams := make(map[string]*Stream)

	wg.Add(len(t.sourceOperatorSpecs))
	for _, o := range t.sourceOperatorSpecs {
		if _, ok := streams[o.produces.name]; !ok {
			streams[o.produces.name] = o.produces
		}
		producerChan := make(chan *Tuple)
		o.produces.AddProducer(o.name, producerChan)
		opCtx := &OperatorContext{
			name: o.name,
			outputCollector: &OutputCollector{
				metadata: &TupleMetadata{
					StreamName: o.produces.name,
					Producer:   o.name,
				},
				output: producerChan,
			},
		}
		go func(ctx context.Context, o *SourceOperatorSpec, opCtx *OperatorContext) {
			o.process(ctx, opCtx)
			close(opCtx.outputCollector.output)
			wg.Done()
		}(ctx, o, opCtx)
	}

	wg.Add(len(t.operatorSpecs))
	for _, o := range t.operatorSpecs {
		consumerChan := make(chan *Tuple)
		o.consumes.AddConsumer(o.name, consumerChan)

		var outputCollector *OutputCollector
		if o.produces != nil {
			if _, ok := streams[o.produces.name]; !ok {
				streams[o.produces.name] = o.produces
			}
			producerChan := make(chan *Tuple)
			outputCollector = &OutputCollector{
				metadata: &TupleMetadata{
					StreamName: o.produces.name,
					Producer:   o.name,
				},
				output: producerChan,
			}
			o.produces.AddProducer(o.name, producerChan)
		}

		opCtx := &OperatorContext{
			name:            o.name,
			outputCollector: outputCollector,
		}
		go func(ctx context.Context, o *OperatorSpec, opCtx *OperatorContext) {
			for tuple := range consumerChan {
				o.process(ctx, opCtx, tuple)
			}
			if opCtx.outputCollector != nil {
				close(opCtx.outputCollector.output)
			}
			wg.Done()
		}(ctx, o, opCtx)
	}

	wg.Add(len(streams))
	for _, stream := range streams {
		go func(s *Stream) {
			s.Run()
			wg.Done()
		}(stream)
	}
	wg.Wait()
	return nil
}

type OperatorSpec struct {
	name        string
	process     ProcessTupleFunc
	parallelism int

	produces *Stream
	consumes *Stream
}

func (o *OperatorSpec) Produces(streams *Stream) *OperatorSpec {
	o.produces = streams
	return o
}

func (o *OperatorSpec) Consumes(streams *Stream) *OperatorSpec {
	o.consumes = streams
	return o
}

type SourceOperatorSpec struct {
	name        string
	process     ProcessFunc
	parallelism int

	produces *Stream
}

func (o *SourceOperatorSpec) Produces(streams *Stream) *SourceOperatorSpec {
	o.produces = streams
	return o
}
