package conductor

// OperatorContext is passed to user-defined ProcessFunc and ProcessTupleFunc
// functions to provide the operator name and tuple submission functionality.
type OperatorContext struct {
	name     string
	instance int
	log      InfoDebugLogger
	outputs  []*OutputPort
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
func (o *OperatorContext) Submit(t *Tuple, port int) {
	op := o.outputs[port]
	t.Metadata.StreamName = op.StreamName()
	t.Metadata.Producer = o.name
	t.Metadata.Instance = o.instance
	op.Submit(t)
}

// NumPorts returns the number of output ports defined in the topology. This
// can be useful to enable a function supporting a configurable number of
// producer streams.
func (o *OperatorContext) NumPorts() int {
	return len(o.outputs)
}
