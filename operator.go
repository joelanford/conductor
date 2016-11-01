package conductor

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
)

// OperatorContext is passed to user-defined ProcessFunc and ProcessTupleFunc
// functions to provide the operator name and tuple submission functionality.
type OperatorContext struct {
	name     string
	instance int
	log      InfoDebugLogger
	metadata []*TupleMetadata
	outputs  []chan *Tuple
}

// Name returns the name of the operator
func (o *OperatorContext) Name() string {
	return o.name
}

func (o *OperatorContext) Instance() int {
	return o.instance
}

// Log returns the InfoDebugLogger instance associated with the operator. It is
// used to log informational and debug messages to the console.
func (o *OperatorContext) Log() InfoDebugLogger {
	return o.log
}

// Submit sends a tuple to the stream on the specified port. The port index
// counts from 0 and corresponds to the order that streams were defined by the
// SourceOperator or Operator instance's Produces() function. Submitting on
// an undefined port will result in a panic.
func (o *OperatorContext) Submit(t TupleData, port int) {
	o.outputs[port] <- &Tuple{Metadata: o.metadata[port], Data: t}
}

// NumPorts returns the number of output ports defined in the topology. This
// can be useful to enable a function supporting a configurable number of
// producer streams.
func (o *OperatorContext) NumPorts() int {
	return len(o.outputs)
}

// ProcessFunc functions are defined when instantiating SourceOperator
// instances and are called by the Topology fabric when it runs the
// SourceOperator instances.  This function should return when there
// are no further tuples to create, or when the context is Done.
//
// The OperatorContext provides operator name and tuple submission
// functionality, and instance is the index of the SourceOperator instance
// dictated by the SourceOperator parallelism.
type ProcessFunc func(ctx context.Context, opCtx OperatorContext)

// ProcessTupleFunc functions are defined when instantiating Operator instances
// and are called by the Topology fabric when it runs the  Operator instances.
// This function should return when it is finished processing the tuple. Any
// goroutines started in the function should return when the context is Done.
//
// The OperatorContext provides operator name and tuple submission
// functionality, and instance is the index of the Operator instance dictated
// by the SourceOperator parallelism.
type ProcessTupleFunc func(ctx context.Context, opCtx OperatorContext, tuple Tuple, instance int)

// Operator encapsulates the necessary information and functionality to perform
// operations on a stream (or streams) of incoming tuples
type Operator struct {
	name        string
	process     ProcessTupleFunc
	parallelism int
	debug       bool

	produces []*Stream
	consumes []*Stream
}

// Produces is used to assign Stream instances to the Operator on which it will
// send tuples to downstream consumers
func (o *Operator) Produces(streams ...*Stream) *Operator {
	o.produces = streams
	return o
}

// Consumes is used to assign Stream instances to the Operator on which it will
// receive tuples from upstream consumers
func (o *Operator) Consumes(streams ...*Stream) *Operator {
	o.consumes = streams
	return o
}

func (o *Operator) SetDebug(debug bool) *Operator {
	o.debug = debug
	return o
}

func (o *Operator) run(ctx context.Context) {
	var wg sync.WaitGroup

	inputPorts := make([]*inputPort, len(o.consumes))
	for i, c := range o.consumes {
		inputPorts[i] = newInputPort(c.consumers[o.name], o.parallelism, PartitionRoundRobin(), 1000)
	}

	wg.Add(len(inputPorts) * (o.parallelism + 1))

	for portNum, ip := range inputPorts {
		go func(ip *inputPort) {
			ip.run()
			wg.Done()
		}(ip)
		for instance := 0; instance < o.parallelism; instance++ {
			go func(ip *inputPort, portNum int, instance int) {
				opCtx := &OperatorContext{
					name:     o.name,
					instance: instance,
					log:      NewLogger(os.Stdout, fmt.Sprintf("%s[%d] ", o.name, instance), log.LstdFlags|log.LUTC),
					metadata: []*TupleMetadata{},
					outputs:  []chan *Tuple{},
				}
				opCtx.log.SetDebug(o.debug)

				for _, p := range o.produces {
					opCtx.metadata = append(opCtx.metadata, &TupleMetadata{
						Producer:   fmt.Sprintf("%s[%d]", o.name, instance),
						StreamName: p.name,
					})
					opCtx.outputs = append(opCtx.outputs, p.producers[o.name])
				}
				for tuple := range ip.getOutput(instance) {
					o.process(ctx, *opCtx, *tuple, portNum)
				}
				wg.Done()
			}(ip, portNum, instance)
		}
	}
	wg.Wait()
	for _, p := range o.produces {
		close(p.producers[o.name])
	}
}

// SourceOperator encapsulates the necessary information and functionality to
// create tuples from an external source (e.g. network socket, file, channel,
// etc.)
type SourceOperator struct {
	name        string
	process     ProcessFunc
	parallelism int
	debug       bool

	produces []*Stream
}

// Produces is used to assign Stream instances to the SourceOperator on which
// it will send tuples to downstream consumers
func (o *SourceOperator) Produces(streams ...*Stream) *SourceOperator {
	o.produces = streams
	return o
}

func (o *SourceOperator) SetDebug(debug bool) *SourceOperator {
	o.debug = debug
	return o
}

func (o *SourceOperator) run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(o.parallelism)
	for instance := 0; instance < o.parallelism; instance++ {
		go func(ctx context.Context, o *SourceOperator, instance int) {
			opCtx := &OperatorContext{
				name:     o.name,
				instance: instance,
				log:      NewLogger(os.Stdout, fmt.Sprintf("%s[%d] ", o.name, instance), log.LstdFlags|log.LUTC),
				metadata: []*TupleMetadata{},
				outputs:  []chan *Tuple{},
			}
			opCtx.log.SetDebug(o.debug)

			for _, p := range o.produces {
				opCtx.metadata = append(opCtx.metadata, &TupleMetadata{
					Producer:   opCtx.name,
					StreamName: p.name,
				})
				opCtx.outputs = append(opCtx.outputs, p.producers[o.name])
			}
			o.process(ctx, *opCtx)
			wg.Done()
		}(ctx, o, instance)
	}

	wg.Wait()
	for _, p := range o.produces {
		close(p.producers[o.name])
	}
}
