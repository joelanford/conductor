package conductor

import "sync"

// Stream manages the distribution for a named stream of Tuple instances from
// all stream producers to all stream consumers.
type stream struct {
	name string

	// All operators that produce a stream will be assigned the same producer channel
	producers map[string]*outputPort

	// All operators that consume a stream will be assigned their own consumer channel
	consumers map[string]*inputPort
}

// NewStream creates a new Stream instance.
func newStream(name string) *stream {
	return &stream{
		name:      name,
		producers: make(map[string]*outputPort),
		consumers: make(map[string]*inputPort),
	}
}

func (s *stream) registerConsumer(name string, partition PartitionFunc, parallelism, queueSize int) *inputPort {
	if _, present := s.consumers[name]; present {
		panic("input port with name " + name + " already exists")
	}
	consumer := newInputPort(s.name, name, partition, parallelism, queueSize)
	s.consumers[name] = consumer
	return consumer
}

func (s *stream) registerProducer(name string) *outputPort {
	if _, present := s.producers[name]; present {
		panic("output port with name " + name + " already exists")
	}
	producer := newOutputPort(s.name, name)
	s.producers[name] = producer
	return producer
}

func (s *stream) run() {
	for tuple := range s.mergeProducers() {
		for _, ip := range s.consumers {
			ip.input <- tuple
		}
	}
	for _, ip := range s.consumers {
		close(ip.input)
	}

}

func (s *stream) mergeProducers() <-chan *Tuple {
	var wg sync.WaitGroup
	out := make(chan *Tuple)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(p <-chan *Tuple) {
		for n := range p {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(s.producers))
	for _, p := range s.producers {
		go output(p.channel)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
