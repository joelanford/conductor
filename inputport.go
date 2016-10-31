package conductor

import (
	"bytes"
	"encoding/gob"
	"hash/fnv"
	"math"
	"math/rand"
	"time"
)

// PartitionFunc functions are used to define how tuples are distributed to
// parallel instances running within an operator.
//
// PartitionFunc functions return an integer that determines the parallel
// instance that will receive the given Tuple. The returned integer is modulo'd
// with the parallelism of the operator to direct the tuple to the correct
// parallel instance.
type PartitionFunc func(*Tuple) int

// PartitionRoundRobinPartitioner implementats a round-robin partitioning
// scheme that sends tuples to parallel operator instances in order of their
// index in increasing order.
func PartitionRoundRobin() PartitionFunc {
	n := uint32(math.MaxUint32)
	return func(t *Tuple) int {
		n++
		return int(n)
	}
}

// PartitionRandom implemetations a random parititioning scheme that sends
// tuples to random parallel operator instances, using Go's builtin math/rand
// package.
func PartitionRandom() PartitionFunc {
	rand.Seed(time.Now().UnixNano())
	return func(t *Tuple) int {
		return rand.Intn(math.MaxInt64)
	}
}

// PartitionHash implements a partition scheme that sends tuples to parallel
// operator instances based on the computed hash of the values of a
// user-defined set of fields.
func PartitionHash(fieldNames ...string) PartitionFunc {
	return func(t *Tuple) int {
		fields := make([]interface{}, len(fieldNames))
		for i, name := range fieldNames {
			fields[i] = t.Data[name]
		}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		_ = enc.Encode(fields)
		h := fnv.New32()
		h.Write(buf.Bytes())
		return int(h.Sum32())
	}
}

type inputPort struct {
	input     <-chan *Tuple
	outputs   []chan *Tuple
	partition PartitionFunc
}

func newInputPort(input <-chan *Tuple, parallelism int, partitionFunc PartitionFunc, queueSize int) *inputPort {
	outputs := make([]chan *Tuple, parallelism)
	for i := 0; i < parallelism; i++ {
		outputs[i] = make(chan *Tuple, queueSize)
	}
	return &inputPort{
		input:     input,
		outputs:   outputs,
		partition: partitionFunc,
	}
}

func (i *inputPort) run() {
	for t := range i.input {
		i.outputs[i.partition(t)%len(i.outputs)] <- t
	}
	for _, o := range i.outputs {
		close(o)
	}
}

func (i *inputPort) getOutput(o int) <-chan *Tuple {
	return i.outputs[o]
}
