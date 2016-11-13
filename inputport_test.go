package conductor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInputPort(t *testing.T) {
	parallelisms := []int{1, 10, 100}
	for _, parallelism := range parallelisms {
		t.Run(fmt.Sprintf("parallelism=%d", parallelism), func(t *testing.T) {
			ip := newInputPort("stream", "operator", PartitionRoundRobin(), parallelism, 1)
			go ip.run()

			if parallelism == 1 {
				assert.Equal(t, ip.input, ip.outputs[0])
			} else {
				assert.Equal(t, parallelism, len(ip.outputs))
			}

			for j := 0; j < 100; j++ {
				in := &Tuple{Data: map[string]interface{}{"value": j}}
				ip.input <- in
				out := <-ip.outputs[j%parallelism]
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
			ip := newInputPort("stream", "operator", PartitionRoundRobin(), parallelism, 1)
			go ip.run()

			for i := 0; i < b.N; i++ {
				ip.input <- in
				<-ip.outputs[i%parallelism]
			}
			close(ip.input)
		})
	}
}
