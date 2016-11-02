package operators

import (
	"context"

	"github.com/joelanford/conductor"
)

type FilterFunc func(conductor.Tuple) bool

type Filter struct {
	oc     conductor.OperatorContext
	filter FilterFunc
}

func NewFilter(filter FilterFunc) conductor.CreateBoltProcessorFunc {
	return func() conductor.BoltProcessor {
		return &Filter{
			filter: filter,
		}
	}
}

func (b *Filter) Setup(ctx context.Context, oc conductor.OperatorContext) {
	b.oc = oc
}
func (b *Filter) Process(ctx context.Context, t conductor.Tuple, port int) {
	if b.filter(t) {
		b.oc.Submit(t.Data, 0)
	} else if b.oc.NumPorts() > 1 {
		b.oc.Submit(t.Data, 1)
	}
}
func (b *Filter) Teardown() {}
