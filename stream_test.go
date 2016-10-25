package conductor

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStream(t *testing.T) {
	p1 := make(chan *Tuple, 1)
	p2 := make(chan *Tuple, 1)
	c1 := make(chan *Tuple, 1)
	c2 := make(chan *Tuple, 1)

	s := NewStream("test")
	s.AddProducer("p1", p1)
	s.AddProducer("p2", p2)
	s.AddConsumer("c1", c1)
	s.AddConsumer("c2", c2)

	t1 := &Tuple{Data: map[string]interface{}{"foo": "bar"}}
	t2 := &Tuple{Data: map[string]interface{}{"whiz": "bang"}}

	var runWg sync.WaitGroup
	var t1Wg sync.WaitGroup
	var t2Wg sync.WaitGroup
	runWg.Add(1)
	t1Wg.Add(2)
	t2Wg.Add(2)
	go func() {
		s.Run()
		runWg.Done()
	}()
	go func() {
		assert.Equal(t, t1, <-c1)
		t1Wg.Done()
		assert.Equal(t, t2, <-c1)
		t2Wg.Done()
	}()
	go func() {
		assert.Equal(t, t1, <-c2)
		t1Wg.Done()
		assert.Equal(t, t2, <-c2)
		t2Wg.Done()
	}()
	p1 <- t1
	t1Wg.Wait()
	p2 <- t2
	t2Wg.Wait()
	close(p1)
	close(p2)
	runWg.Wait()
}
