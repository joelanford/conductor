package streams

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOutputPort(t *testing.T) {
	output := make(chan *Tuple, 1)
	op := newOutputPort("stream", "operator", 0, output)

	assert.Equal(t, "stream", op.streamName)
	assert.Equal(t, "operator", op.operatorName)

	for i := 0; i < 100; i++ {
		in := &Tuple{Data: map[string]interface{}{"value": i}}
		op.output <- in
		out := <-output
		assert.Equal(t, in, out)
	}
	close(op.output)
}

func BenchmarkOutputPort(b *testing.B) {
	in := &Tuple{Data: map[string]interface{}{"value": 1}}
	output := make(chan *Tuple, 1)
	op := newOutputPort("stream", "operator", 0, output)

	for i := 0; i < b.N; i++ {
		op.output <- in
		<-output
	}
	close(op.output)

}
