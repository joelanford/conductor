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

type Partitioner interface {
	Partition(*Tuple) int
}

type RoundRobinPartitioner struct {
	i int
}

func (p *RoundRobinPartitioner) Partition(t *Tuple) int {
	rr := p.i
	p.i++
	return rr
}

type RandomPartitioner struct{}

func (p *RandomPartitioner) Partition(t *Tuple) int {
	return rand.Intn(math.MaxInt64)
}

type HashPartitioner struct {
	fieldNames []string
}

func NewHashPartitioner(fieldNames ...string) *HashPartitioner {
	return &HashPartitioner{fieldNames: fieldNames}
}

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
