package conductor_test

import (
	"testing"

	"github.com/joelanford/conductor"
	"github.com/stretchr/testify/assert"
)

func TestStreamOneToOne(t *testing.T) {
	s := conductor.NewStream("test")
	p := s.RegisterProducer("p")
	c := s.RegisterConsumer("c", 0)
	tuple := &conductor.Tuple{Data: map[string]interface{}{"a": 1}}

	assert.Equal(t, 1, len(s.Producers()))
	assert.Equal(t, 1, len(s.Consumers()))

	go s.Run()

	p <- tuple
	close(p)
	assert.Equal(t, tuple, <-c)

}

func TestStreamOneToMany(t *testing.T) {
	s := conductor.NewStream("test")
	p := s.RegisterProducer("p")
	c1 := s.RegisterConsumer("c1", 0)
	c2 := s.RegisterConsumer("c2", 0)
	c3 := s.RegisterConsumer("c3", 0)
	tuple1 := &conductor.Tuple{Data: map[string]interface{}{"a": 1}}

	assert.Equal(t, 1, len(s.Producers()))
	assert.Equal(t, 3, len(s.Consumers()))

	go s.Run()

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
	s := conductor.NewStream("test")
	p1 := s.RegisterProducer("p1")
	p2 := s.RegisterProducer("p2")
	p3 := s.RegisterProducer("p3")
	c := s.RegisterConsumer("c", 0)
	tuple1 := &conductor.Tuple{Data: map[string]interface{}{"a": 1}}
	tuple2 := &conductor.Tuple{Data: map[string]interface{}{"a": 2}}
	tuple3 := &conductor.Tuple{Data: map[string]interface{}{"a": 3}}

	assert.Equal(t, 3, len(s.Producers()))
	assert.Equal(t, 1, len(s.Consumers()))

	go s.Run()

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
	s := conductor.NewStream("test")
	p1 := s.RegisterProducer("p1")
	p2 := s.RegisterProducer("p2")
	p3 := s.RegisterProducer("p3")
	c1 := s.RegisterConsumer("c1", 0)
	c2 := s.RegisterConsumer("c2", 0)
	c3 := s.RegisterConsumer("c3", 0)
	tuple1 := &conductor.Tuple{Data: map[string]interface{}{"a": 1}}
	tuple2 := &conductor.Tuple{Data: map[string]interface{}{"a": 2}}
	tuple3 := &conductor.Tuple{Data: map[string]interface{}{"a": 3}}

	assert.Equal(t, 3, len(s.Producers()))
	assert.Equal(t, 3, len(s.Consumers()))

	go s.Run()

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
	s := conductor.NewStream("test")
	p := s.RegisterProducer("p")
	c := s.RegisterConsumer("c", 100)
	tuple := &conductor.Tuple{Data: map[string]interface{}{"a": 1}}

	go s.Run()

	for i := 0; i < b.N; i++ {
		p <- tuple
		<-c
	}
	close(p)
}
