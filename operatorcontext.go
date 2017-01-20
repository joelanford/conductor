package streams

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// OperatorContext is passed to user-defined ProcessFunc and ProcessTupleFunc
// functions to provide the operator name and tuple submission functionality.
type OperatorContext struct {
	name             string
	instance         int
	parallelism      int
	log              InfoDebugLogger
	outputs          []*outputPort
	metricsCollector *MetricsCollector

	numberLookup map[int]string

	sendMetricDelta  float64
	sendMetricTicker *time.Ticker
}

func newOperatorContext(name string, instance int, log InfoDebugLogger, outputs []*outputPort, mc *MetricsCollector) *OperatorContext {
	numberLookup := make(map[int]string)
	numberLookup[instance] = strconv.Itoa(instance)
	for i := range outputs {
		numberLookup[i] = strconv.Itoa(i)
	}

	oc := &OperatorContext{
		name:             name,
		instance:         instance,
		log:              log,
		outputs:          outputs,
		metricsCollector: mc,
		numberLookup:     numberLookup,
		sendMetricTicker: time.NewTicker(500 * time.Millisecond),
	}
	return oc
}

// Name returns the name of the operator
func (o *OperatorContext) Name() string {
	return o.name
}

func (o *OperatorContext) Instance() int {
	return o.instance
}

func (o *OperatorContext) InstanceString() string {
	return o.numberLookup[o.instance]
}

func (o *OperatorContext) Parallelism() int {
	return o.parallelism
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
func (o *OperatorContext) Submit(t *Tuple, port int) {
	op := o.outputs[port]
	t.Metadata.StreamName = op.streamName
	t.Metadata.Producer = o.name
	t.Metadata.Instance = o.instance
	o.sendMetricDelta++
	select {
	case <-o.sendMetricTicker.C:
		op.tuplesSent.WithLabelValues(t.Metadata.Producer, o.numberLookup[o.instance], t.Metadata.StreamName, o.numberLookup[port]).Add(o.sendMetricDelta)
		o.sendMetricDelta = 0.0
	default:
	}
	op.output <- t
}

// NumPorts returns the number of output ports defined in the topology. This
// can be useful to enable a function supporting a configurable number of
// producer streams.
func (o *OperatorContext) NumPorts() int {
	return len(o.outputs)
}

func (o *OperatorContext) RegisterMetric(m prometheus.Collector) {
	o.metricsCollector.Register(m)
}

func (o *OperatorContext) SetDebug(debug bool) {
	o.log.SetDebug(debug)
}
