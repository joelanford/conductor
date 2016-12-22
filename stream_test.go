package streams

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamOneToOne(t *testing.T) {
	s := NewStream("test")
	p := s.registerProducer("p")
	c := s.registerConsumer("c", 0)
	tuple := &Tuple{Data: map[string]interface{}{"a": 1}}

	assert.Equal(t, 1, len(s.producers))
	assert.Equal(t, 1, len(s.consumers))

	go s.run()

	p <- tuple
	close(p)
	assert.Equal(t, tuple, <-c)

}

func TestStreamOneToMany(t *testing.T) {
	s := NewStream("test")
	p := s.registerProducer("p")
	c1 := s.registerConsumer("c1", 0)
	c2 := s.registerConsumer("c2", 0)
	c3 := s.registerConsumer("c3", 0)
	tuple1 := &Tuple{Data: map[string]interface{}{"a": 1}}

	assert.Equal(t, 1, len(s.producers))
	assert.Equal(t, 3, len(s.consumers))

	go s.run()

	p <- tuple1
	close(p)
	for i := 0; i < 3; i++ {
		select {
		case tuple := <-c1:
			assert.Equal(t, tuple1, tuple)
		case tuple := <-c2:
			assert.Equal(t, tuple1, tuple)
		case tuple := <-c3:
			assert.Equal(t, tuple1, tuple)
		}
	}
}

func TestStreamManyToOne(t *testing.T) {
	s := NewStream("test")
	p1 := s.registerProducer("p1")
	p2 := s.registerProducer("p2")
	p3 := s.registerProducer("p3")
	c := s.registerConsumer("c", 0)
	tuple1 := &Tuple{Data: map[string]interface{}{"a": 1}}
	tuple2 := &Tuple{Data: map[string]interface{}{"a": 2}}
	tuple3 := &Tuple{Data: map[string]interface{}{"a": 3}}

	assert.Equal(t, 3, len(s.producers))
	assert.Equal(t, 1, len(s.consumers))

	go s.run()

	p1 <- tuple1
	close(p1)
	assert.Equal(t, tuple1, <-c)

	p2 <- tuple2
	close(p2)
	assert.Equal(t, tuple2, <-c)

	p3 <- tuple3
	close(p3)
	assert.Equal(t, tuple3, <-c)
}

func TestStreamManyToMany(t *testing.T) {
	s := NewStream("test")
	p1 := s.registerProducer("p1")
	p2 := s.registerProducer("p2")
	p3 := s.registerProducer("p3")
	c1 := s.registerConsumer("c1", 0)
	c2 := s.registerConsumer("c2", 0)
	c3 := s.registerConsumer("c3", 0)
	tuple1 := &Tuple{Data: map[string]interface{}{"a": 1}}
	tuple2 := &Tuple{Data: map[string]interface{}{"a": 2}}
	tuple3 := &Tuple{Data: map[string]interface{}{"a": 3}}

	assert.Equal(t, 3, len(s.producers))
	assert.Equal(t, 3, len(s.consumers))

	go s.run()

	p1 <- tuple1
	close(p1)
	for i := 0; i < 3; i++ {
		select {
		case tuple := <-c1:
			assert.Equal(t, tuple1, tuple)
		case tuple := <-c2:
			assert.Equal(t, tuple1, tuple)
		case tuple := <-c3:
			assert.Equal(t, tuple1, tuple)
		}
	}

	p2 <- tuple2
	close(p2)
	for i := 0; i < 3; i++ {
		select {
		case tuple := <-c1:
			assert.Equal(t, tuple2, tuple)
		case tuple := <-c2:
			assert.Equal(t, tuple2, tuple)
		case tuple := <-c3:
			assert.Equal(t, tuple2, tuple)
		}
	}

	p3 <- tuple3
	close(p3)
	for i := 0; i < 3; i++ {
		select {
		case tuple := <-c1:
			assert.Equal(t, tuple3, tuple)
		case tuple := <-c2:
			assert.Equal(t, tuple3, tuple)
		case tuple := <-c3:
			assert.Equal(t, tuple3, tuple)
		}
	}
}

func BenchmarkStream(b *testing.B) {
	s := NewStream("test")
	p := s.registerProducer("p")
	c := s.registerConsumer("c", 100)
	tuple := &Tuple{Data: map[string]interface{}{"a": 1}}

	go s.run()

	for i := 0; i < b.N; i++ {
		p <- tuple
		<-c
	}
	close(p)
}
