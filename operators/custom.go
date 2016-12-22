package operators

import (
	"context"

	"github.com/joelanford/streams"
)

type CustomFunc func(*streams.OperatorContext, *streams.Tuple, int)

type Custom struct {
	oc     *streams.OperatorContext
	custom CustomFunc
}

func NewCustom(custom CustomFunc) streams.CreateBoltProcessorFunc {
	return func() streams.BoltProcessor {
		return &Custom{
			custom: custom,
		}
	}
}

func (b *Custom) Setup(ctx context.Context, oc *streams.OperatorContext) {
	b.oc = oc
}

func (b *Custom) Process(ctx context.Context, t *streams.Tuple, port int) {
	b.custom(b.oc, t, port)
}

func (b *Custom) Teardown() {}
