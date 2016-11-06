package operators

import (
	"context"

	"github.com/joelanford/conductor"
)

type CustomFunc func(*conductor.OperatorContext, *conductor.Tuple, int)

type Custom struct {
	oc     *conductor.OperatorContext
	custom CustomFunc
}

func NewCustom(custom CustomFunc) conductor.CreateBoltProcessorFunc {
	return func() conductor.BoltProcessor {
		return &Custom{
			custom: custom,
		}
	}
}

func (b *Custom) Setup(ctx context.Context, oc *conductor.OperatorContext) {
	b.oc = oc
}

func (b *Custom) Process(ctx context.Context, t *conductor.Tuple, port int) {
	b.custom(b.oc, t, port)
}

func (b *Custom) Teardown() {}
