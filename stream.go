package conductor

import "sync"

type Consumer struct {
	Name    string
	Channel <-chan *Tuple
}

type Producer struct {
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

func (s *Stream) RegisterConsumer(name string, queueSize int) chan *Tuple {
	if _, present := s.consumers[name]; present {
		panic("consumer with name " + name + " already exists")
	}
	consumer := make(chan *Tuple, queueSize)
	s.consumers[name] = consumer
	return consumer
}

func (s *Stream) Consumers() []Consumer {
	consumers := make([]Consumer, len(s.consumers))
	i := 0
	for name, channel := range s.consumers {
		consumers[i] = Consumer{Name: name, Channel: channel}
		i++
	}
	return consumers
}

func (s *Stream) RegisterProducer(name string) chan *Tuple {
	if _, present := s.producers[name]; present {
		panic("producer with name " + name + " already exists")
	}
	producer := make(chan *Tuple)
	s.producers[name] = producer
	return producer
}

func (s *Stream) Producers() []Producer {
	producers := make([]Producer, len(s.producers))
	i := 0
	for name, channel := range s.producers {
		producers[i] = Producer{Name: name, Channel: channel}
	}
	return producers
}

func (s *Stream) Run() {
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
