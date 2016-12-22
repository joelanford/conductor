package operators

import (
	"context"

	"github.com/joelanford/streams"
)

type FilterFunc func(*streams.Tuple) bool

type Filter struct {
	oc     *streams.OperatorContext
	filter FilterFunc
}

func NewFilter(filter FilterFunc) streams.CreateBoltProcessorFunc {
	return func() streams.BoltProcessor {
		return &Filter{
			filter: filter,
		}
	}
}

func (b *Filter) Setup(ctx context.Context, oc *streams.OperatorContext) {
	b.oc = oc
}
func (b *Filter) Process(ctx context.Context, t *streams.Tuple, port int) {
	if b.filter(t) {
		b.oc.Submit(t, 0)
	} else if b.oc.NumPorts() > 1 {
		b.oc.Submit(t, 1)
	}
}
func (b *Filter) Teardown() {}
