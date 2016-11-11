package conductor

import (
	"context"
	"sync"

	"io/ioutil"

	"github.com/awalterschulze/gographviz"
)

// Topology is the top-level entry point of Conductor.  It is used to create
// stream processing pipelines consisting of Spout and Bolt
// instances connected by Stream instances.
type Topology struct {
	name    string
	spouts  []Spout
	bolts   []Bolt
	streams map[string]*stream
	debug   bool
}

// NewTopology creates a new Topology instance
func NewTopology(name string) *Topology {
	return &Topology{
		name:    name,
		spouts:  make([]Spout, 0),
		bolts:   make([]Bolt, 0),
		streams: make(map[string]*stream),
		debug:   false,
	}
}

// AddSpout creates and adds a new Spout instance to the
// topology. This function returns a Spout instance, which is used
// to declare the streams that the Spout instance produces.
func (t *Topology) AddSpout(name string, createProcessor CreateSpoutProcessorFunc, parallelism int) *Spout {
	o := Spout{
		name:            name,
		createProcessor: createProcessor,
		parallelism:     parallelism,
		debug:           false,
		topology:        t,
	}
	t.spouts = append(t.spouts, o)
	return &t.spouts[len(t.spouts)-1]
}

// AddBolt creates and adds a new Bolt instance to the topology. This
// function returns an Bolt instance, which is used to declare the streams
// that the Bolt instance consumes and produces.
func (t *Topology) AddBolt(name string, createProcessor CreateBoltProcessorFunc, parallelism int) *Bolt {
	o := Bolt{
		name:            name,
		createProcessor: createProcessor,
		parallelism:     parallelism,
		debug:           false,
		topology:        t,
	}
	t.bolts = append(t.bolts, o)
	return &t.bolts[len(t.bolts)-1]
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
		go func(s *stream) {
			s.run()
			wg.Done()
		}(s)
	}

	// Run all of the spouts
	wg.Add(len(t.spouts))
	for _, o := range t.spouts {
		go func(o Spout) {
			o.debug = o.debug || t.debug
			o.run(ctx)
			wg.Done()
		}(o)
	}

	// Run all of the bolts
	wg.Add(len(t.bolts))
	for _, o := range t.bolts {
		go func(o Bolt) {
			o.debug = o.debug || t.debug
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

func (t *Topology) Graph(ofile string) {
	graph := gographviz.NewGraph()
	graph.Name = t.name
	graph.Directed = true
	graph.Attrs.Add("rankdir", "LR")
	for _, s := range t.streams {
		graph.AddNode(t.name, s.name, map[string]string{"shape": "rarrow"})
		for p, _ := range s.producers {
			graph.AddEdge(p, s.name, true, nil)
		}
		for c, _ := range s.consumers {
			graph.AddEdge(s.name, c, true, nil)
		}
	}

	for _, o := range t.spouts {
		graph.AddNode(t.name, o.name, map[string]string{"shape": "ellipse"})
	}

	for _, o := range t.bolts {
		if len(o.outputs) == 0 {
			graph.AddNode(t.name, o.name, map[string]string{"shape": "ellipse"})
		} else {
			graph.AddNode(t.name, o.name, map[string]string{"shape": "rectangle"})
		}
	}
	ioutil.WriteFile(ofile, []byte(graph.String()), 0644)
}
