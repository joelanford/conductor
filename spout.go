package conductor

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
)

// ProcessFunc functions are defined when instantiating SourceOperator
// instances and are called by the Topology fabric when it runs the
// SourceOperator instances.  This function should return when there
// are no further tuples to create, or when the context is Done.
//
// The OperatorContext provides operator name and tuple submission
// functionality, and instance is the index of the SourceOperator instance
// dictated by the SourceOperator parallelism.
type ProcessFunc func(ctx context.Context, opCtx OperatorContext)

// Spout encapsulates the necessary information and functionality to
// create tuples from an external source (e.g. network socket, file, channel,
// etc.)
type Spout struct {
	name        string
	process     ProcessFunc
	parallelism int
	debug       bool

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
			opCtx := &OperatorContext{
				name:     o.name,
				instance: instance,
				log:      NewLogger(os.Stdout, fmt.Sprintf("%s[%d] ", o.name, instance), log.LstdFlags|log.LUTC),
				outputs:  o.outputs,
			}
			opCtx.log.SetDebug(o.debug)
			o.process(ctx, *opCtx)
			wg.Done()
		}(ctx, o, instance)
	}

	wg.Wait()
	for _, output := range o.outputs {
		close(output.channel)
	}
}
