package conductor

type outputPort struct {
	name    string
	channel chan *Tuple
}

func newOutputPort(name string) *outputPort {
	return &outputPort{
		name:    name,
		channel: make(chan *Tuple),
	}
}
