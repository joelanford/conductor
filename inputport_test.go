package conductor_test

import (
	"fmt"
	"testing"

	"github.com/joelanford/conductor"
	"github.com/stretchr/testify/assert"
)

func TestInputPort(t *testing.T) {
	parallelisms := []int{1, 10, 100}
	for _, parallelism := range parallelisms {
		t.Run(fmt.Sprintf("parallelism=%d", parallelism), func(t *testing.T) {
			input := make(chan *conductor.Tuple, 1)
			ip := conductor.NewInputPort("stream", "operator", conductor.PartitionRoundRobin(), parallelism, input)
			go ip.Run()

			assert.Equal(t, "stream", ip.StreamName())
			assert.Equal(t, "operator", ip.OperatorName())
			assert.Equal(t, parallelism, ip.NumOutputs())

			for i := 0; i < 100; i++ {
				in := &conductor.Tuple{Data: map[string]interface{}{"value": i}}
				ip.Input() <- in
				out := <-ip.Output(i % parallelism)
				assert.Equal(t, in, out)
			}
			ip.Close()
		})
	}
}

func BenchmarkInputPort(b *testing.B) {
	in := &conductor.Tuple{Data: map[string]interface{}{"value": 1}}

	parallelisms := []int{1, 10, 100}
	for _, parallelism := range parallelisms {
		b.Run(fmt.Sprintf("parallelism=%d", parallelism), func(b *testing.B) {
			input := make(chan *conductor.Tuple, 1)
			ip := conductor.NewInputPort("stream", "operator", conductor.PartitionRoundRobin(), parallelism, input)
			go ip.Run()

			for i := 0; i < b.N; i++ {
				ip.Input() <- in
				<-ip.Output(i % parallelism)
			}
			ip.Close()
		})
	}
}
