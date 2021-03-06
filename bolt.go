package streams

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	Setup(context.Context, *OperatorContext)
	Process(context.Context, *Tuple, int)
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

	tuplesReceived   *prometheus.CounterVec
	metricsCollector *MetricsCollector
}

func newBolt(t *Topology, name string, createProcessor CreateBoltProcessorFunc, parallelism int) *Bolt {
	tuplesReceived := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "streams",
		Name:      "tuples_received_total",
		Help:      "The total number of tuples recevied by an operator in a streams topology",
	}, []string{"operator", "instance", "stream", "port"})

	metricsCollector := NewMetricsCollector()
	metricsCollector.Register(tuplesReceived)

	return &Bolt{
		name:             name,
		createProcessor:  createProcessor,
		parallelism:      parallelism,
		debug:            false,
		topology:         t,
		tuplesReceived:   tuplesReceived,
		metricsCollector: metricsCollector,
	}
}

// Produces is used to register streams to the Bolt, which
// it will use to send tuples to downstream consumers
func (o *Bolt) Produces(streams ...*Stream) *Bolt {
	for _, stream := range streams {
		output := stream.registerProducer(o.name)
		op := newOutputPort(stream.Name(), o.name, len(o.outputs), output)
		o.outputs = append(o.outputs, op)

		o.metricsCollector.Register(op.tuplesSent)
	}
	return o
}

// ConsumesPartitioned is used to register streams to the Bolt, which it will
// use to receive tuples from upstream producers. It also allows users to
// specify a custom partitioning function.
func (o *Bolt) ConsumesPartitioned(stream *Stream, partition PartitionFunc, queueSize int) *Bolt {
	input := stream.registerConsumer(o.name, queueSize)
	o.inputs = append(o.inputs, newInputPort(stream.Name(), o.name, partition, o.parallelism, input))
	return o
}

// Consumes is used to register streams to the Bolt, which it will use to
// receive tuples from upstream producers. If the operator parallelism is
// greater than one, round robin partitioning will automatically be used.
func (o *Bolt) Consumes(stream *Stream, queueSize int) *Bolt {
	var partition PartitionFunc
	if o.parallelism > 1 {
		partition = PartitionRoundRobin()
	}
	input := stream.registerConsumer(o.name, queueSize)

	o.inputs = append(o.inputs, newInputPort(stream.Name(), o.name, partition, o.parallelism, input))
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

	wg.Add(o.parallelism)
	for instance := 0; instance < o.parallelism; instance++ {
		go func(instance int) {
			oc := newOperatorContext(
				o.name,
				instance,
				NewLogger(os.Stdout, fmt.Sprintf("%s[%d] ", o.name, instance), log.LstdFlags|log.Lmicroseconds|log.LUTC),
				o.outputs,
				o.metricsCollector,
			)
			oc.SetDebug(o.debug)
			processor := o.createProcessor()
			processor.Setup(ctx, oc)

			var inputWg sync.WaitGroup
			instanceStr := strconv.Itoa(instance)
			inputWg.Add(len(o.inputs))
			for portNum, ip := range o.inputs {
				go func(ip *inputPort, portNum int) {
					rcvMetricTicker := time.NewTicker(500 * time.Millisecond)
					rcvMetricDelta := 0.0
					portNumStr := strconv.Itoa(portNum)
					for tuple := range ip.outputs[instance] {
						rcvMetricDelta++
						select {
						case <-rcvMetricTicker.C:
							o.tuplesReceived.WithLabelValues(o.name, instanceStr, ip.streamName, portNumStr).Add(rcvMetricDelta)
							rcvMetricDelta = 0.0
						default:
						}
						processor.Process(ctx, tuple, portNum)
					}
					rcvMetricTicker.Stop()
					inputWg.Done()
				}(ip, portNum)
			}
			inputWg.Wait()
			processor.Teardown()
			oc.sendMetricTicker.Stop()
			wg.Done()
		}(instance)
	}

	wg.Wait()
	for _, output := range o.outputs {
		close(output.output)
	}
}
