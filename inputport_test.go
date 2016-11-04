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
	p := PartitionHash("a", "b", "c", "d")
	for i := 0; i < b.N; i++ {
		p(tuples[i%4])
	}
}

func TestInputPortRoundRobinNoOverflow(t *testing.T) {
	for i := 1; i <= 100; i++ {
		ip := newInputPort(PartitionRoundRobin(), i, i)
		go ip.run()

		for j := 0; j < i; j++ {
			in := &Tuple{Data: map[string]interface{}{"value": i}}
			ip.input <- in
			out := <-ip.outputs[j]
			assert.Equal(t, in, out)
		}
		close(ip.input)
	}
}

func TestInputPortRoundRobinOverflow(t *testing.T) {
	ip := newInputPort(PartitionRoundRobin(), 4, 100)
	go ip.run()

	for j := 0; j < 100; j++ {
		in := &Tuple{Data: map[string]interface{}{"value": j}}
		ip.input <- in
		out := <-ip.outputs[j%4]
		assert.Equal(t, in, out)
	}
	close(ip.input)
}

func BenchmarkInputPortNoParallelism(b *testing.B) {
	ip := newInputPort(nil, 1, 1)
	in := &Tuple{Data: map[string]interface{}{"value": 1}}
	go ip.run()

	for i := 0; i < b.N; i++ {
		ip.input <- in
		<-ip.outputs[0]
	}
	close(ip.input)
}

func BenchmarkInputPortParallelism(b *testing.B) {
	ip := newInputPort(PartitionRoundRobin(), 100, 1)
	in := &Tuple{Data: map[string]interface{}{"value": 1}}
	go ip.run()

	for i := 0; i < b.N; i++ {
		ip.input <- in
		<-ip.outputs[i%100]
	}
	close(ip.input)
}
