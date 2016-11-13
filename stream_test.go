package conductor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamOneToOne(t *testing.T) {
	s := newStream("test")
	p := s.registerProducer("p", 0)
	c := s.registerConsumer("c", PartitionRoundRobin(), 1, 0)

	go s.run()

	tuple := &Tuple{Data: map[string]interface{}{"a": 1}}
	p.submit(tuple)
	p.close()
	assert.Equal(t, tuple, <-c.input)
}

func TestStreamOneToMany(t *testing.T) {
	s := newStream("test")
	p := s.registerProducer("p", 0)
	c1 := s.registerConsumer("c1", PartitionRoundRobin(), 1, 0)
	c2 := s.registerConsumer("c2", PartitionRoundRobin(), 1, 0)
	c3 := s.registerConsumer("c3", PartitionRoundRobin(), 1, 0)

	go s.run()

	tuple1 := &Tuple{Data: map[string]interface{}{"a": 1}}
	p.submit(tuple1)
	p.close()
	for i := 0; i < 3; i++ {
		select {
		case tuple := <-c1.input:
			assert.Equal(t, tuple1, tuple)
		case tuple := <-c2.input:
			assert.Equal(t, tuple1, tuple)
		case tuple := <-c3.input:
			assert.Equal(t, tuple1, tuple)
		}
	}
}

func TestStreamManyToOne(t *testing.T) {
	s := newStream("test")
	p1 := s.registerProducer("p1", 0)
	p2 := s.registerProducer("p2", 1)
	p3 := s.registerProducer("p3", 2)
	c := s.registerConsumer("c", PartitionRoundRobin(), 1, 0)

	go s.run()

	tuple1 := &Tuple{Data: map[string]interface{}{"a": 1}}
	tuple2 := &Tuple{Data: map[string]interface{}{"a": 2}}
	tuple3 := &Tuple{Data: map[string]interface{}{"a": 3}}

	p1.submit(tuple1)
	p1.close()
	assert.Equal(t, tuple1, <-c.input)

	p2.submit(tuple2)
	p2.close()
	assert.Equal(t, tuple2, <-c.input)

	p3.submit(tuple3)
	p3.close()
	assert.Equal(t, tuple3, <-c.input)
}

func TestStreamManyToMany(t *testing.T) {
	s := newStream("test")
	p1 := s.registerProducer("p1", 0)
	p2 := s.registerProducer("p2", 1)
	p3 := s.registerProducer("p3", 2)
	c1 := s.registerConsumer("c1", PartitionRoundRobin(), 1, 0)
	c2 := s.registerConsumer("c2", PartitionRoundRobin(), 1, 0)
	c3 := s.registerConsumer("c3", PartitionRoundRobin(), 1, 0)

	go s.run()

	tuple1 := &Tuple{Data: map[string]interface{}{"a": 1}}
	tuple2 := &Tuple{Data: map[string]interface{}{"a": 2}}
	tuple3 := &Tuple{Data: map[string]interface{}{"a": 3}}

	p1.submit(tuple1)
	p1.close()
	for i := 0; i < 3; i++ {
		select {
		case tuple := <-c1.input:
			assert.Equal(t, tuple1, tuple)
		case tuple := <-c2.input:
			assert.Equal(t, tuple1, tuple)
		case tuple := <-c3.input:
			assert.Equal(t, tuple1, tuple)
		}
	}

	p2.submit(tuple2)
	p2.close()
	for i := 0; i < 3; i++ {
		select {
		case tuple := <-c1.input:
			assert.Equal(t, tuple2, tuple)
		case tuple := <-c2.input:
			assert.Equal(t, tuple2, tuple)
		case tuple := <-c3.input:
			assert.Equal(t, tuple2, tuple)
		}
	}

	p3.submit(tuple3)
	p3.close()
	for i := 0; i < 3; i++ {
		select {
		case tuple := <-c1.input:
			assert.Equal(t, tuple3, tuple)
		case tuple := <-c2.input:
			assert.Equal(t, tuple3, tuple)
		case tuple := <-c3.input:
			assert.Equal(t, tuple3, tuple)
		}
	}
}

func BenchmarkStream(b *testing.B) {
	s := newStream("test")
	p := s.registerProducer("p", 0)
	c := s.registerConsumer("c", PartitionRoundRobin(), 1, 100)

	go s.run()

	tuple := &Tuple{Data: map[string]interface{}{"a": 1}}
	for i := 0; i < b.N; i++ {
		p.submit(tuple)
		<-c.input
	}
	p.close()
}
