package conductor

import (
	"context"
	"sync"
)

// Topology is the top-level entry point of Conductor.  It is used to create
// stream processing pipelines consisting of SourceOperator and Operator
// instances connected by Stream instances.
type Topology struct {
	name            string
	sourceOperators []*SourceOperator
	operators       []*Operator
	streams         map[string]*Stream
	debug           bool
}

// NewTopology creates a new Topology instance
func NewTopology(name string) *Topology {
	return &Topology{
		name:            name,
		sourceOperators: make([]*SourceOperator, 0),
		operators:       make([]*Operator, 0),
		streams:         make(map[string]*Stream),
		debug:           false,
	}
}

// AddSourceOperator creates and adds a new SourceOperator instance to the
// topology. This function returns a SourceOperator instance, which is used
// to declare the streams that the SourceOperator instance produces.
func (t *Topology) AddSourceOperator(name string, process ProcessFunc, parallelism int) *SourceOperator {
	o := &SourceOperator{
		name:        name,
		process:     process,
		parallelism: parallelism,
		debug:       false,
	}
	t.sourceOperators = append(t.sourceOperators, o)
	return o
}

// AddOperator creates and adds a new Operator instance to the topology. This
// function returns an Operator instance, which is used to declare the streams
// that the Operator instance consumes and produces.
func (t *Topology) AddOperator(name string, process ProcessTupleFunc, parallelism int) *Operator {
	o := &Operator{
		name:        name,
		process:     process,
		parallelism: parallelism,
		debug:       false,
	}
	t.operators = append(t.operators, o)
	return o
}

// Run executes the Topology instance.  This function should only be called
// after all SourceOperator and Operator intances have been added and have had
// their streams declared.  This function should not be called concurrently
// for the same Topology instance. The passed in context can be used to cancel
// the Topology before all SourceOperators have completed.
func (t *Topology) Run(ctx context.Context) error {
	// This WaitGroup is used to wait for all operators and streams to complete
	// before returning from this function.
	var wg sync.WaitGroup

	// It is very important that all stream channels are created and added to
	// the streams before starting the stream. Otherwise the stream will not
	// receive all upstream tuples sent to it or it will not properly forward
	// tuples to all consumers. It can also cause deadlocks and other
	// unintended consequences.
	t.setupStreams()

	// Run all of the streams
	wg.Add(len(t.streams))
	for _, s := range t.streams {
		go func(s *Stream) {
			s.run()
			wg.Done()
		}(s)
	}

	// Run all of the source operators
	wg.Add(len(t.sourceOperators))
	for _, o := range t.sourceOperators {
		go func(o *SourceOperator) {
			o.debug = o.debug || t.debug
			o.run(ctx)
			wg.Done()
		}(o)
	}

	// Run all of the operators
	wg.Add(len(t.operators))
	for _, o := range t.operators {
		go func(o *Operator) {
			o.debug = o.debug || t.debug
			o.run(ctx)
			wg.Done()
		}(o)
	}

	// Wait for all streams, source operators, and operators to complete.
	wg.Wait()
	return nil
}

func (t *Topology) SetDebug(debug bool) *Topology {
	t.debug = debug
	return t
}

func (t *Topology) setupStreams() {
	for _, o := range t.sourceOperators {
		for _, p := range o.produces {
			p.addProducer(o.name, make(chan *Tuple))
			if _, ok := t.streams[p.name]; !ok {
				t.streams[p.name] = p
			}
		}
	}

	for _, o := range t.operators {
		for _, p := range o.produces {
			p.addProducer(o.name, make(chan *Tuple))
			if _, ok := t.streams[p.name]; !ok {
				t.streams[p.name] = p
			}
		}
		for _, c := range o.consumes {
			c.addConsumer(o.name, make(chan *Tuple))
		}
	}
}
