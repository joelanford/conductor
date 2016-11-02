package conductor

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
)

// CreateSpoutProcessorFunc is used by the topology to create SpoutProcessor
// instances for each instance in a parallel region.
type CreateSpoutProcessorFunc func() SpoutProcessor

// SpoutProcessor defines the interface for user-defined Spout processing.
// After the topology creates the SpoutProcessor using the
// CreateSpoutProcessorFunc, it calls, Setup, Process, and Teardown in order.
//
// Setup should be used to initialize the SpoutProcessor, for example to set
// struct variables, initialize state, etc. Any goroutines started in Setup
// should be stopped by Teardown or when the context is done.
//
// Process should be used to create and submit tuples from an external source.
// Any goroutines started in Process should be stopped by Teardown or when the
// context is done.
//
// Teardown should be used to stop any remaining goroutines, and perform any
// other necessary cleanup.
type SpoutProcessor interface {
	Setup(context.Context, OperatorContext, int)
	Process(context.Context)
	Teardown()
}

// Spout encapsulates the necessary information and functionality to
// create tuples from an external source (e.g. network socket, file, channel,
// etc.)
type Spout struct {
	name            string
	createProcessor CreateSpoutProcessorFunc
	parallelism     int
	debug           bool

	topology *Topology

	outputs []*outputPort
}

// Produces is used to register streams to the Spout, which
// it will use to send tuples to downstream consumers
func (o *Spout) Produces(streamNames ...string) *Spout {
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

// SetDebug turns debug logging on.
func (o *Spout) SetDebug(debug bool) *Spout {
	o.debug = debug
	return o
}

func (o *Spout) run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(o.parallelism)
	for instance := 0; instance < o.parallelism; instance++ {
		go func(ctx context.Context, o *Spout, instance int) {
			oc := OperatorContext{
				name:     o.name,
				instance: instance,
				log:      NewLogger(os.Stdout, fmt.Sprintf("%s[%d] ", o.name, instance), log.LstdFlags|log.LUTC),
				outputs:  o.outputs,
			}
			oc.log.SetDebug(o.debug)
			processor := o.createProcessor()
			processor.Setup(ctx, oc, instance)
			processor.Process(ctx)
			processor.Teardown()
			wg.Done()
		}(ctx, o, instance)
	}

	wg.Wait()
	for _, output := range o.outputs {
		close(output.channel)
	}
}
