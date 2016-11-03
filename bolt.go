package conductor

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
)

// CreateBoltProcessorFunc is used by the topology to create BoltProcessor
// instances for each instance in a parallel region.
type CreateBoltProcessorFunc func() BoltProcessor

// BoltProcessor defines the interface for user-defined Bolt processing.
// After the topology creates the BoltProcessor using the
// CreateBoltProcessorFunc, it calls, Setup, Process (for every tuple until the
// input streams are closed), and Teardown in order.
//
// Setup should be used to initialize the BoltProcessor, for example to set
// struct variables, initialize state, etc. Any goroutines started in Setup
// should be stopped by Teardown or when the context is done.
//
// Process should be used to process tuples from the Bolt's input ports. Any
// goroutines started in Process should be stopped by Teardown or when the
// context is done.
//
// Teardown should be used to stop any remaining goroutines, and perform any
// other necessary cleanup.
type BoltProcessor interface {
	Setup(context.Context, OperatorContext)
	Process(context.Context, Tuple, int)
	Teardown()
}

// Bolt encapsulates the necessary information and functionality to perform
// operations on a stream (or streams) of incoming tuples
type Bolt struct {
	name            string
	createProcessor CreateBoltProcessorFunc
	parallelism     int
	debug           bool

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

	wg.Add(len(o.inputs))
	for _, ip := range o.inputs {
		go func(ip *inputPort) {
			ip.run()
			wg.Done()
		}(ip)
	}

	wg.Add(o.parallelism * len(o.inputs))
	for instance := 0; instance < o.parallelism; instance++ {
		go func(instance int) {
			oc := OperatorContext{
				name:     o.name,
				instance: instance,
				log:      NewLogger(os.Stdout, fmt.Sprintf("%s[%d] ", o.name, instance), log.LstdFlags|log.LUTC),
				outputs:  o.outputs,
			}
			oc.log.SetDebug(o.debug)

			processor := o.createProcessor()

			processor.Setup(ctx, oc)

			for portNum, ip := range o.inputs {
				go func(ip *inputPort, portNum int) {
					for tuple := range ip.outputs[instance] {
						processor.Process(ctx, *tuple, portNum)
					}
					wg.Done()
				}(ip, portNum)
			}

			processor.Teardown()
			wg.Done()
		}(instance)
	}

	wg.Wait()
	for _, output := range o.outputs {
		close(output.channel)
	}
}
