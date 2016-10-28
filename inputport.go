package conductor

import (
	"bytes"
	"encoding/gob"
	"hash/fnv"
	"math"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Partitioner is used to define how tuples are distributed to parallel
// instances running within an operator.
type Partitioner interface {
	// Partition returns an integer that determines the parallel instance
	// that will receive the given Tuple. The returned integer is modulo'd with
	// the parallelism of the operator to direct the tuple to the correct
	// parallel instance.
	Partition(*Tuple) int
}

// RoundRobinPartitioner is a Partitioner implementation that sends tuples
// to parallel operator instances in order of their index in increasing order.
type RoundRobinPartitioner struct {
	i int
}

// Partition increments a counter and returns it to perform a simple
// round-robin paritioning scheme.
func (p *RoundRobinPartitioner) Partition(t *Tuple) int {
	rr := p.i
	p.i++
	return rr
}

// RandomPartitioner is a Partitioner implemetation that sends tuples to random
// parallel operator instances, using Go's builtin rand package.
type RandomPartitioner struct{}

// Partition returns a random integer to perform a random partitioning scheme.
func (p *RandomPartitioner) Partition(t *Tuple) int {
	return rand.Intn(math.MaxInt64)
}

// HashPartitioner is a Partitioner implementation that sends tuples to
// parallel operator instances based on the computed hash of the values of a
// user-defined set of fields.
type HashPartitioner struct {
	fieldNames []string
}

// NewHashPartitioner creates a new HashPartitioner instance with the given
// fieldNames, which are used to compute a hash for their values.
func NewHashPartitioner(fieldNames ...string) *HashPartitioner {
	return &HashPartitioner{fieldNames: fieldNames}
}

// Partition computes a hash value (using FNV) by creating a new map containing
// the values of the defined fields, gob-encoding the map, and then computing
// and returning the FNV hash of the gob-encoded bytes.
func (p *HashPartitioner) Partition(t *Tuple) int {
	fields := make([]interface{}, len(p.fieldNames))
	for i, name := range p.fieldNames {
		fields[i] = t.Data[name]
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(fields)
	h := fnv.New32()
	h.Write(buf.Bytes())
	return int(h.Sum32())
}

type inputPort struct {
	input       <-chan *Tuple
	outputs     []chan *Tuple
	partitioner Partitioner
}

func newInputPort(input <-chan *Tuple, parallelism int, partitioner Partitioner, queueSize int) *inputPort {
	outputs := make([]chan *Tuple, parallelism)
	for i := 0; i < parallelism; i++ {
		outputs[i] = make(chan *Tuple, queueSize)
	}
	return &inputPort{
		input:       input,
		outputs:     outputs,
		partitioner: partitioner,
	}
}

func (i *inputPort) run() {
	for t := range i.input {
		i.outputs[i.partitioner.Partition(t)%len(i.outputs)] <- t
	}
	for _, o := range i.outputs {
		close(o)
	}
}

func (i *inputPort) getOutput(o int) <-chan *Tuple {
	return i.outputs[o]
}
