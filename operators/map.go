package operators

import (
	"context"

	"github.com/joelanford/streams"
)

type MapFunc func(*streams.Tuple) *streams.Tuple

type Map struct {
	oc     *streams.OperatorContext
	mapper MapFunc
}

func NewMap(mapper MapFunc) streams.CreateBoltProcessorFunc {
	return func() streams.BoltProcessor {
		return &Map{
			mapper: mapper,
		}
	}
}

func (b *Map) Setup(ctx context.Context, oc *streams.OperatorContext) {
	b.oc = oc
}

func (b *Map) Process(ctx context.Context, t *streams.Tuple, port int) {
	b.oc.Submit(b.mapper(t), 0)
}

func (b *Map) Teardown() {}
