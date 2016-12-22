package streams

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInputPort(t *testing.T) {
	parallelisms := []int{1, 10, 100}
	for _, parallelism := range parallelisms {
		t.Run(fmt.Sprintf("parallelism=%d", parallelism), func(t *testing.T) {
			input := make(chan *Tuple, 1)
			ip := newInputPort("stream", "operator", PartitionRoundRobin(), parallelism, input)
			go ip.run()

			assert.Equal(t, "stream", ip.streamName)
			assert.Equal(t, "operator", ip.operatorName)
			assert.Equal(t, parallelism, len(ip.outputs))

			for i := 0; i < 100; i++ {
				in := &Tuple{Data: map[string]interface{}{"value": i}}
				ip.input <- in
				out := <-ip.outputs[i%parallelism]
				assert.Equal(t, in, out)
			}
			close(ip.input)
		})
	}
}

func BenchmarkInputPort(b *testing.B) {
	in := &Tuple{Data: map[string]interface{}{"value": 1}}

	parallelisms := []int{1, 10, 100}
	for _, parallelism := range parallelisms {
		b.Run(fmt.Sprintf("parallelism=%d", parallelism), func(b *testing.B) {
			input := make(chan *Tuple, 1)
			ip := newInputPort("stream", "operator", PartitionRoundRobin(), parallelism, input)
			go ip.run()

			for i := 0; i < b.N; i++ {
				ip.input <- in
				<-ip.outputs[i%parallelism]
			}
			close(ip.input)
		})
	}
}
