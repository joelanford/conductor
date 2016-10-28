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
			p.addProducer(o.name, producerChans[i])
			if _, ok := t.streams[p.name]; !ok {
				t.streams[p.name] = p
			}
		}
		go func(o *SourceOperator) {
			o.run(ctx, t, producerChans)
			for _, output := range producerChans {
				close(output)
			}
			wg.Done()
		}(o)
	}

	wg.Add(len(t.operators))
	for _, o := range t.operators {
		inputPorts := make([]*inputPort, len(o.consumes))
		for i, c := range o.consumes {
			consumerChan := make(chan *Tuple)
			c.addConsumer(o.name, consumerChan)

			// TODO: make partitioner and queue size configurable through topology API
			inputPorts[i] = newInputPort(consumerChan, o.parallelism, &RoundRobinPartitioner{}, 1000)
		}
		producerChans := make([]chan *Tuple, len(o.produces))
		for i, p := range o.produces {
			producerChans[i] = make(chan *Tuple)
			p.addProducer(o.name, producerChans[i])
			if _, ok := t.streams[p.name]; !ok {
				t.streams[p.name] = p
			}
		}
		go func(o *Operator) {
			o.run(ctx, t, producerChans, inputPorts)
			for _, output := range producerChans {
				close(output)
			}
			wg.Done()
		}(o)
	}

	wg.Add(len(t.streams))
	for _, s := range t.streams {
		go func(s *Stream) {
			s.run()
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

func (o *Operator) run(ctx context.Context, t *Topology, producerChans []chan *Tuple, inputPorts []*inputPort) {
	var wg sync.WaitGroup
	wg.Add(len(inputPorts) * (o.parallelism + 1))
	for portNum, ip := range inputPorts {
		go func(ip *inputPort) {
			ip.run()
			wg.Done()
		}(ip)
		for instance := 0; instance < o.parallelism; instance++ {
			go func(ip *inputPort, portNum int, instance int) {
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
				for tuple := range ip.getOutput(instance) {
					o.process(ctx, *opCtx, *tuple, portNum)
				}
				wg.Done()
			}(ip, portNum, instance)
		}
	}
	wg.Wait()
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

func (o *SourceOperator) run(ctx context.Context, t *Topology, producerChans []chan *Tuple) {
	var wg sync.WaitGroup
	wg.Add(o.parallelism)
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
			wg.Done()
		}(ctx, o, instance)
	}

	wg.Wait()
}
