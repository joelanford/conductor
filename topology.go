package conductor

import (
	"context"
	"fmt"
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
		producerChans := make([]chan *Tuple, len(o.produces))
		for i, p := range o.produces {
			producerChans[i] = make(chan *Tuple)
			if _, ok := streams[p.name]; !ok {
				streams[p.name] = p
			}
			p.AddProducer(o.name, producerChans[i])
		}
		var producerWg sync.WaitGroup
		producerWg.Add(o.parallelism)
		for instance := 0; instance < o.parallelism; instance++ {
			go func(ctx context.Context, o *SourceOperatorSpec, instance int) {
				opCtx := &OperatorContext{
					name: fmt.Sprintf("%s[%d]", o.name, instance),
					outputCollector: &OutputCollector{
						metadata: []*TupleMetadata{},
						outputs:  producerChans,
					},
				}
				for _, p := range o.produces {
					opCtx.outputCollector.metadata = append(opCtx.outputCollector.metadata, &TupleMetadata{
						Producer:   opCtx.name,
						StreamName: p.name,
					})
				}
				o.process(ctx, *opCtx, instance)
				producerWg.Done()
			}(ctx, o, instance)
		}
		go func() {
			producerWg.Wait()
			for _, output := range producerChans {
				close(output)
			}
			wg.Done()
		}()
	}

	wg.Add(len(t.operatorSpecs))
	for _, o := range t.operatorSpecs {
		inputPorts := make([]*InputPort, len(o.consumes))
		for i, c := range o.consumes {
			consumerChan := make(chan *Tuple)
			c.AddConsumer(o.name, consumerChan)

			// TODO: make partitioner and queue size configurable through topology API
			inputPorts[i] = NewInputPort(consumerChan, o.parallelism, &RoundRobinPartitioner{}, 1000)
		}

		producerChans := make([]chan *Tuple, len(o.produces))
		for i, p := range o.produces {
			producerChans[i] = make(chan *Tuple)
			if _, ok := streams[p.name]; !ok {
				streams[p.name] = p
			}
			p.AddProducer(o.name, producerChans[i])
		}

		go func(ctx context.Context, o *OperatorSpec) {
			var consumerWg sync.WaitGroup
			consumerWg.Add(len(inputPorts) * o.parallelism)
			for portNum, inputPort := range inputPorts {
				go inputPort.Run()
				for instance := 0; instance < o.parallelism; instance++ {
					go func(inputPort *InputPort, portNum int, instance int) {
						opCtx := &OperatorContext{
							name: fmt.Sprintf("%s[%d]", o.name, instance),
							outputCollector: &OutputCollector{
								metadata: []*TupleMetadata{},
								outputs:  producerChans,
							},
						}
						for _, p := range o.produces {
							opCtx.outputCollector.metadata = append(opCtx.outputCollector.metadata, &TupleMetadata{
								Producer:   opCtx.name,
								StreamName: p.name,
							})
						}
						for tuple := range inputPort.GetOutput(instance) {
							o.process(ctx, *opCtx, *tuple, portNum)
						}
						consumerWg.Done()
					}(inputPort, portNum, instance)
				}
			}
			consumerWg.Wait()
			for _, output := range producerChans {
				close(output)
			}
			wg.Done()
		}(ctx, o)
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

	produces []*Stream
	consumes []*Stream
}

func (o *OperatorSpec) Produces(streams ...*Stream) *OperatorSpec {
	o.produces = streams
	return o
}

func (o *OperatorSpec) Consumes(streams ...*Stream) *OperatorSpec {
	o.consumes = streams
	return o
}

type SourceOperatorSpec struct {
	name        string
	process     ProcessFunc
	parallelism int

	produces []*Stream
}

func (o *SourceOperatorSpec) Produces(streams ...*Stream) *SourceOperatorSpec {
	o.produces = streams
	return o
}
