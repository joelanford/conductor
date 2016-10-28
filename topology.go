package conductor

import (
	"context"
	"fmt"
	"sync"
)

type Topology struct {
	name            string
	sourceOperators []*SourceOperator
	operators       []*Operator
	streams         map[string]*Stream
}

func NewTopology(name string) *Topology {
	return &Topology{
		name:            name,
		sourceOperators: make([]*SourceOperator, 0),
		operators:       make([]*Operator, 0),
		streams:         make(map[string]*Stream),
	}
}

func (t *Topology) AddSourceOperator(name string, process ProcessFunc, parallelism int) *SourceOperator {
	o := &SourceOperator{
		name:        name,
		process:     process,
		parallelism: parallelism,
	}
	t.sourceOperators = append(t.sourceOperators, o)
	return o
}

func (t *Topology) AddOperator(name string, process ProcessTupleFunc, parallelism int) *Operator {
	o := &Operator{
		name:        name,
		process:     process,
		parallelism: parallelism,
	}
	t.operators = append(t.operators, o)
	return o
}

func (t *Topology) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Add(len(t.sourceOperators))
	for _, o := range t.sourceOperators {
		producerChans := make([]chan *Tuple, len(o.produces))
		for i, p := range o.produces {
			producerChans[i] = make(chan *Tuple)
			if _, ok := t.streams[p.name]; !ok {
				t.streams[p.name] = p
			}
			p.AddProducer(o.name, producerChans[i])
		}
		var producerWg sync.WaitGroup
		producerWg.Add(o.parallelism)
		for instance := 0; instance < o.parallelism; instance++ {
			go func(ctx context.Context, o *SourceOperator, instance int) {
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

	wg.Add(len(t.operators))
	for _, o := range t.operators {
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
			if _, ok := t.streams[p.name]; !ok {
				t.streams[p.name] = p
			}
			p.AddProducer(o.name, producerChans[i])
		}

		go func(ctx context.Context, o *Operator) {
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

	wg.Add(len(t.streams))
	for _, s := range t.streams {
		go func(s *Stream) {
			s.Run()
			wg.Done()
		}(s)
	}
	wg.Wait()
	return nil
}

type Operator struct {
	name        string
	process     ProcessTupleFunc
	parallelism int

	produces []*Stream
	consumes []*Stream
}

func (o *Operator) Produces(streams ...*Stream) *Operator {
	o.produces = streams
	return o
}

func (o *Operator) Consumes(streams ...*Stream) *Operator {
	o.consumes = streams
	return o
}

type SourceOperator struct {
	name        string
	process     ProcessFunc
	parallelism int

	produces []*Stream
}

func (o *SourceOperator) Produces(streams ...*Stream) *SourceOperator {
	o.produces = streams
	return o
}
