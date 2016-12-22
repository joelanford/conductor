package operators

import (
	"context"

	"github.com/joelanford/streams"
)

type TupleLogger struct {
	oc *streams.OperatorContext
}

func NewTupleLogger() streams.CreateBoltProcessorFunc {
	return func() streams.BoltProcessor {
		return &TupleLogger{}
	}
}

func (b *TupleLogger) Setup(ctx context.Context, oc *streams.OperatorContext) {
	b.oc = oc
}
func (b *TupleLogger) Process(ctx context.Context, t *streams.Tuple, port int) {
	b.oc.Log().Infof("%+v %+v", t.Metadata, t.Data)
}
func (b *TupleLogger) Teardown() {}
