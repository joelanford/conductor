package streams

import (
	"context"
	"sync"
)

// Topology is the top-level entry point of streams.  It is used to create
// stream processing pipelines consisting of Spout and Bolt
// instances connected by Stream instances.
type Topology struct {
	name    string
	spouts  []*Spout
	bolts   []*Bolt
	streams map[string]*Stream

	debug bool
	mutex sync.Mutex
}

// NewTopology creates a new Topology instance
func NewTopology(name string) *Topology {
	return &Topology{
		name:    name,
		spouts:  make([]*Spout, 0),
		bolts:   make([]*Bolt, 0),
		streams: make(map[string]*Stream),
		debug:   false,
	}
}

func (t *Topology) AddStream(name string) *Stream {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	stream, ok := t.streams[name]
	if !ok {
		stream = NewStream(name)
		t.streams[name] = stream
	}
	return stream
}

// AddSpout creates and adds a new Spout instance to the
// topology. This function returns a Spout instance, which is used
// to declare the streams that the Spout instance produces.
func (t *Topology) AddSpout(name string, createProcessor CreateSpoutProcessorFunc, parallelism int) *Spout {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	o := newSpout(t, name, createProcessor, parallelism)
	t.spouts = append(t.spouts, o)
	return o
}

// AddBolt creates and adds a new Bolt instance to the topology. This
// function returns an Bolt instance, which is used to declare the streams
// that the Bolt instance consumes and produces.
func (t *Topology) AddBolt(name string, createProcessor CreateBoltProcessorFunc, parallelism int) *Bolt {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	o := newBolt(t, name, createProcessor, parallelism)
	t.bolts = append(t.bolts, o)
	return o
}

// Run executes the Topology instance.  This function should only be called
// after all Spout and Bolt intances have been added and have had
// their streams declared.  This function should not be called concurrently
// for the same Topology instance. The passed in context can be used to cancel
// the Topology before all spouts have completed.
func (t *Topology) Run(ctx context.Context) error {
	// This WaitGroup is used to wait for all bolts and streams to complete
	// before returning from this function.
	var wg sync.WaitGroup

	// Run all of the streams
	wg.Add(len(t.streams))
	for _, s := range t.streams {
		go func(s *Stream) {
			s.run()
			wg.Done()
		}(s)
	}

	// Run all of the spouts
	wg.Add(len(t.spouts))
	for _, o := range t.spouts {
		go func(o *Spout) {
			if t.debug {
				o.SetDebug(true)
			}
			o.run(ctx)
			wg.Done()
		}(o)
	}

	// Run all of the bolts
	wg.Add(len(t.bolts))
	for _, o := range t.bolts {
		go func(o *Bolt) {
			if t.debug {
				o.SetDebug(true)
			}
			o.run(ctx)
			wg.Done()
		}(o)
	}

	// Wait for all streams, spouts, and bolts to complete.
	wg.Wait()
	return nil
}

func (t *Topology) SetDebug(debug bool) *Topology {
	t.debug = debug
	return t
}
