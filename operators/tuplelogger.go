package operators

import (
	"context"

	"github.com/joelanford/conductor"
)

type TupleLogger struct {
	oc       conductor.OperatorContext
	instance int
}

func NewTupleLogger() conductor.CreateBoltProcessorFunc {
	return func() conductor.BoltProcessor {
		return &TupleLogger{}
	}
}

func (b *TupleLogger) Setup(ctx context.Context, oc conductor.OperatorContext, instance int) {
	b.oc = oc
	b.instance = instance
}
func (b *TupleLogger) Process(ctx context.Context, t conductor.Tuple, port int) {
	b.oc.Log().Infof("%+v %+v", t.Metadata, t.Data)
}
func (b *TupleLogger) Teardown() {}
