package conductor

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundRobinPartitioner(t *testing.T) {
	p := PartitionRoundRobin()
	tuple := &Tuple{}
	for i := 0; i < 1000; i++ {
		assert.Equal(t, i, p(tuple))
	}
}

func BenchmarkRoundRobinPartitioner(b *testing.B) {
	p := PartitionRoundRobin()
	tuple := &Tuple{}
	for i := 0; i < b.N; i++ {
		p(tuple)
	}
}

func TestRandomPartitioner(t *testing.T) {
	for i := int64(1); i < 5; i++ {
		rand.Seed(i)
		vals := make([]int, 1000)
		for j := int64(0); j < 1000; j++ {
			vals[j] = rand.Intn(math.MaxInt64)
		}
		p := PartitionRandom()

		// need to reset rand.Seed since PartitionRandom automatically changes the seed.
		rand.Seed(i)
		tuple := &Tuple{}
		for j := int64(0); j < 1000; j++ {
			assert.Equal(t, vals[j], p(tuple))
		}
	}
}

func BenchmarkRandomPartitioner(b *testing.B) {
	rand.Seed(1)
	p := PartitionRandom()
	tuple := &Tuple{}
	for i := 0; i < b.N; i++ {
		p(tuple)
	}
}

func TestHashPartitioner(t *testing.T) {
	tuples := []*Tuple{
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 1}},
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 2}},
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 3}},
		&Tuple{Data: map[string]interface{}{"a": 1, "b": 4}},
	}
	pa := PartitionHash("a")
	pab := PartitionHash("a", "b")

	for i := 0; i < 4; i++ {
		for j := 0; j <= i; j++ {
			assert.Equal(t, pa(tuples[i]), pa(tuples[j]))
			if i == j {
				assert.Equal(t, pab(tuples[i]), pab(tuples[j]))
			} else {
				assert.NotEqual(t, pab(tuples[i]), pab(tuples[j]))
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
	p := PartitionHash("a", "b", "c")
	for i := 0; i < b.N; i++ {
		p(tuples[i%4])
	}
}
