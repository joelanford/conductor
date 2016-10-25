package conductor

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRoundRobinPartitioner tests the RoundRobinPartitioner implementation.
// It iteratates 1000 times and verifies that the RoundRobinPartitioner
// Partition() function returns incrementing integers.
func TestRoundRobinPartitioner(t *testing.T) {
	p := &RoundRobinPartitioner{}
	tuple := &Tuple{}
	for i := 0; i < 1000; i++ {
		assert.Equal(t, i, p.Partition(tuple))
	}
}

func BenchmarkRoundRobinPartitioner(b *testing.B) {
	p := &RoundRobinPartitioner{}
	tuple := &Tuple{}
	for i := 0; i < b.N; i++ {
		p.Partition(tuple)
	}
}

// TestRandomPartitioner tests the RandomPartitioner implementation with
// 5 different seeds.  During each loop, it seeds the randomness, collects
// 1000 random values, then reseeds the randomness and validates that the
// RandomPartitioner Partition() function returns the same values.
func TestRandomPartitioner(t *testing.T) {
	for i := int64(1); i < 5; i++ {
		rand.Seed(i)
		vals := make([]int, 1000)
		for j := int64(0); j < 1000; j++ {
			vals[j] = rand.Intn(math.MaxInt64)
		}
		rand.Seed(i)
		p := &RandomPartitioner{}
		tuple := &Tuple{}
		for j := int64(0); j < 1000; j++ {
			assert.Equal(t, vals[j], p.Partition(tuple))
		}
	}
}

func BenchmarkRandomPartitioner(b *testing.B) {
	rand.Seed(1)
	p := &RandomPartitioner{}
	tuple := &Tuple{}
	for i := 0; i < b.N; i++ {
		p.Partition(tuple)
	}
}

// TestHashPartitioner tests the HashPartitioner implementation. It test two
// instances of the HashPartitioner with four tuples with "a" values that are
// equal and "b" values that are not. The first HashPartitioner hashes only
// the "a" values, so we expect Partition to return the same value for all
// tuples.  The second HashPartitioner hashes both "a" and "b" values, so we
// expect Partition to return different values for each tuple.
func TestHashPartitioner(t *testing.T) {
	tuples := []*Tuple{
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 1}},
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 2}},
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 3}},
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 4}},
	}
	pa := NewHashPartitioner("a")
	pab := NewHashPartitioner("a", "b")

	for i := 0; i < 4; i++ {
		for j := 0; j <= i; j++ {
			assert.Equal(t, pa.Partition(tuples[i]), pa.Partition(tuples[j]))
			if i == j {
				assert.Equal(t, pab.Partition(tuples[i]), pab.Partition(tuples[j]))
			} else {
				assert.NotEqual(t, pab.Partition(tuples[i]), pab.Partition(tuples[j]))
			}
		}
	}
}

func BenchmarkHashPartitioner(b *testing.B) {
	tuples := []*Tuple{
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 1, "c": "foobar"}},
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 2, "c": "helloworld"}},
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 3, "c": "This is a sentence that is a bit longer than the other \"c\" values"}},
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 4, "c": "1"}},
	}
	p := NewHashPartitioner("a", "b", "c", "d")
	for i := 0; i < b.N; i++ {
		p.Partition(tuples[i%4])
	}
}

func TestInputPortRoundRobinNoOverflow(t *testing.T) {
	for i := 1; i <= 100; i++ {
		input := make(chan *Tuple, i)
		ip := NewInputPort(input, i, &RoundRobinPartitioner{}, i)
		go func() {
			for j := 0; j < i; j++ {
				in := &Tuple{Data: map[string]interface{}{"value": i}}
				input <- in
				out := <-ip.GetOutput(j)
				assert.Equal(t, in, out)
			}
			close(input)
		}()
		ip.Run()
	}
}

func TestInputPortRoundRobinOverflow(t *testing.T) {
	input := make(chan *Tuple, 1)
	ip := NewInputPort(input, 4, &RoundRobinPartitioner{}, 100)
	go func() {
		for j := 0; j < 100; j++ {
			in := &Tuple{Data: map[string]interface{}{"value": j}}
			input <- in
			out := <-ip.GetOutput(j % 4)
			assert.Equal(t, in, out)
		}
		close(input)
	}()
	ip.Run()
}
