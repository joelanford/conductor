package conductor

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamOneToMany(t *testing.T) {
	for i := 1; i < 100; i++ {
		s := NewStream("test")

		p1 := make(chan *Tuple, i)
		s.addProducer("p1", p1)

		c := make([]chan *Tuple, i)
		for j := 0; j < i; j++ {
			c[j] = make(chan *Tuple, i)
			s.addConsumer(fmt.Sprintf("c%d", j), c[j])
		}

		tuples := make([]*Tuple, i)
		for j := 0; j < i; j++ {
			tuples[j] = &Tuple{Data: map[string]interface{}{"a": j + 1}}
		}

		for j := 0; j < i; j++ {
			p1 <- tuples[j]
		}
		close(p1)

		var wg sync.WaitGroup
		wg.Add(i)
		for j := 0; j < i; j++ {
			go func(j int) {
				count := 0

				for tuple := range c[j] {
					count++
					assert.Equal(t, count, tuple.Data["a"])
				}
				assert.Equal(t, count, i)
				wg.Done()
			}(j)
		}
		s.run()
		wg.Wait()
	}
}

func TestStreamManyToOne(t *testing.T) {
	for i := 1; i < 100; i++ {
		s := NewStream("test")

		p := make([]chan *Tuple, i)
		for j := 0; j < i; j++ {
			p[j] = make(chan *Tuple, i)
			s.addProducer(fmt.Sprintf("p%d", j), p[j])
		}

		c := make(chan *Tuple, i)
		s.addConsumer("c", c)

		tuples := make([]*Tuple, i)
		for j := 0; j < i; j++ {
			tuples[j] = &Tuple{Data: map[string]interface{}{"a": j + 1}}
		}

		for j := 0; j < i; j++ {
			p[j] <- tuples[j]
			close(p[j])
		}

		nums := make(map[int]bool)
		for j := 1; j <= i; j++ {
			nums[j] = true
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			count := 0

			for tuple := range c {
				count++
				delete(nums, tuple.Data["a"].(int))
			}
			assert.Equal(t, i, count)
			assert.Empty(t, nums)
			wg.Done()
		}()

		s.run()
		wg.Wait()
	}
}

func TestStream(t *testing.T) {
	p1 := make(chan *Tuple, 1)
	p2 := make(chan *Tuple, 1)
	c1 := make(chan *Tuple, 1)
	c2 := make(chan *Tuple, 1)

	s := NewStream("test")
	s.addProducer("p1", p1)
	s.addProducer("p2", p2)
	s.addConsumer("c1", c1)
	s.addConsumer("c2", c2)

	t1 := &Tuple{Data: map[string]interface{}{"foo": "bar"}}
	t2 := &Tuple{Data: map[string]interface{}{"whiz": "bang"}}

	var runWg sync.WaitGroup
	var t1Wg sync.WaitGroup
	var t2Wg sync.WaitGroup
	runWg.Add(1)
	t1Wg.Add(2)
	t2Wg.Add(2)
	go func() {
		s.run()
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
