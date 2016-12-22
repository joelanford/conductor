package streams

import "sync"

type consumer struct {
	Name    string
	Channel <-chan *Tuple
}

type producer struct {
	Name    string
	Channel chan<- *Tuple
}

// Stream manages the distribution for a named stream of Tuple instances from
// all stream producers to all stream consumers.
type Stream struct {
	name      string
	producers map[string]chan *Tuple
	consumers map[string]chan *Tuple
}

// NewStream creates a new Stream instance.
func NewStream(name string) *Stream {
	return &Stream{
		name:      name,
		producers: make(map[string]chan *Tuple),
		consumers: make(map[string]chan *Tuple),
	}
}

func (s *Stream) Name() string {
	return s.name
}

func (s *Stream) registerConsumer(name string, queueSize int) chan *Tuple {
	if _, present := s.consumers[name]; present {
		panic("consumer with name " + name + " already exists")
	}
	consumer := make(chan *Tuple, queueSize)
	s.consumers[name] = consumer
	return consumer
}

func (s *Stream) registerProducer(name string) chan *Tuple {
	if _, present := s.producers[name]; present {
		panic("producer with name " + name + " already exists")
	}
	producer := make(chan *Tuple)
	s.producers[name] = producer
	return producer
}

func (s *Stream) run() {
	for tuple := range s.mergeProducers() {
		for _, ip := range s.consumers {
			ip <- tuple
		}
	}
	for _, ip := range s.consumers {
		close(ip)
	}
}

func (s *Stream) mergeProducers() <-chan *Tuple {
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
		go output(p)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
