package conductor

import (
	"sync"

	"github.com/pkg/errors"
)

// Stream manages the distribution for a named stream of tuples, providing a producer channel
// to add tuples to the stream and consumer channel registration to provide all consumers their
// own copy of the data.
type Stream struct {
	name string

	// All operators that produce a stream will be assigned the same producer channel
	producers map[string]<-chan *Tuple

	// All operators that consume a stream will be assigned their own consumer channel
	consumers map[string]chan<- *Tuple
}

// NewStream creates a new Stream instance.
func NewStream(name string) *Stream {
	return &Stream{
		name:      name,
		producers: make(map[string]<-chan *Tuple),
		consumers: make(map[string]chan<- *Tuple),
	}
}

// AddConsumer registers the consumer channel with the stream.  If the consumer
// name has already been added, AddConsumer will return an error. Consumer channels
// registered with a Stream will be closed by the Stream when the Stream producers
// are all closed.  If the consumer stream registered is closed elsewhere, the program
// may panic while attempting to write to a closed channel.
func (s *Stream) addConsumer(name string, consumer chan<- *Tuple) error {
	if _, present := s.consumers[name]; present {
		return errors.Errorf("cannot overwrite consumer with name %s", name)
	}
	s.consumers[name] = consumer
	return nil
}

// AddProducer registers the producer channel with the stream.  If the producer
// name has already been added, AddProducer will return an error. Producer channels
// registered with a Stream should be closed externally to notify the stream that no
// further tuples will be sent. If the producer stream registered is not closed, the program
// may not exit cleanly with bounded data streams.
func (s *Stream) addProducer(name string, producer <-chan *Tuple) error {
	if _, present := s.producers[name]; present {
		return errors.Errorf("cannot overwrite producer with name %s", name)
	}
	s.producers[name] = producer
	return nil
}

func (s *Stream) run() {
	for tuple := range s.mergeProducers() {
		for _, c := range s.consumers {
			c <- tuple
		}
	}
	for _, n := range s.consumers {
		close(n)
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
