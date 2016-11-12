package operators

import (
	"context"

	"github.com/joelanford/conductor"
)

type MapFunc func(*conductor.Tuple) *conductor.Tuple

type Map struct {
	oc     *conductor.OperatorContext
	mapper MapFunc
}

func NewMap(mapper MapFunc) conductor.CreateBoltProcessorFunc {
	return func() conductor.BoltProcessor {
		return &Map{
			mapper: mapper,
		}
	}
}

func (b *Map) Setup(ctx context.Context, oc *conductor.OperatorContext) {
	b.oc = oc
}

func (b *Map) Process(ctx context.Context, t *conductor.Tuple, port int) {
	b.oc.Submit(b.mapper(t), 0)
}

func (b *Map) Teardown() {}
