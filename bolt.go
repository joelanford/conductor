package conductor

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
)

// ProcessTupleFunc functions are defined when instantiating Operator instances
// and are called by the Topology fabric when it runs the  Operator instances.
// This function should return when it is finished processing the tuple. Any
// goroutines started in the function should return when the context is Done.
//
// The OperatorContext provides operator name and tuple submission
// functionality, and instance is the index of the Operator instance dictated
// by the SourceOperator parallelism.
type ProcessTupleFunc func(ctx context.Context, opCtx OperatorContext, tuple Tuple, instance int)

// Bolt encapsulates the necessary information and functionality to perform
// operations on a stream (or streams) of incoming tuples
type Bolt struct {
	name        string
	process     ProcessTupleFunc
	parallelism int
	debug       bool

	topology *Topology

	inputs  []*inputPort
	outputs []*outputPort
}

// Produces is used to register streams to the Bolt, which
// it will use to send tuples to downstream consumers
func (o *Bolt) Produces(streamNames ...string) *Bolt {
	for _, streamName := range streamNames {
		stream, ok := o.topology.streams[streamName]
		if !ok {
			stream = newStream(streamName)
			o.topology.streams[streamName] = stream
		}
		o.outputs = append(o.outputs, stream.registerProducer(o.name))
	}
	return o
}

// Consumes is used to register streams to the Bolt, which
// it will use to receive tuples from upstream producers
func (o *Bolt) Consumes(streamName string, partition PartitionFunc, queueSize int) *Bolt {
	stream, ok := o.topology.streams[streamName]
	if !ok {
		stream = newStream(streamName)
		o.topology.streams[streamName] = stream
	}
	o.inputs = append(o.inputs, stream.registerConsumer(o.name, partition, o.parallelism, queueSize))
	return o
}

// SetDebug turns debug logging on.
func (o *Bolt) SetDebug(debug bool) *Bolt {
	o.debug = debug
	return o
}

func (o *Bolt) run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(len(o.inputs) * (o.parallelism + 1))

	for portNum, ip := range o.inputs {
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
					outputs:  o.outputs,
				}
				opCtx.log.SetDebug(o.debug)

				for tuple := range ip.outputs[instance] {
					o.process(ctx, *opCtx, *tuple, portNum)
				}
				wg.Done()
			}(ip, portNum, instance)
		}
	}
	wg.Wait()
	for _, output := range o.outputs {
		close(output.channel)
	}
}
