package conductor_test

import (
	"testing"

	"github.com/joelanford/conductor"
	"github.com/stretchr/testify/assert"
)

func TestOutputPort(t *testing.T) {
	output := make(chan *conductor.Tuple, 1)
	op := conductor.NewOutputPort("stream", "operator", 0, output)

	assert.Equal(t, "stream", op.StreamName())
	assert.Equal(t, "operator", op.OperatorName())

	for i := 0; i < 100; i++ {
		in := &conductor.Tuple{Data: map[string]interface{}{"value": i}}
		op.Submit(in)
		out := <-output
		assert.Equal(t, in, out)
	}
	op.Close()
}

func BenchmarkOutputPort(b *testing.B) {
	in := &conductor.Tuple{Data: map[string]interface{}{"value": 1}}
	output := make(chan *conductor.Tuple, 1)
	op := conductor.NewOutputPort("stream", "operator", 0, output)

	for i := 0; i < b.N; i++ {
		op.Submit(in)
		<-output
	}
	op.Close()

}
