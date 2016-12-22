package operators

import (
	"context"

	"github.com/joelanford/streams"
)

type CustomSpoutFunc func(context.Context, *streams.OperatorContext)

type CustomSpout struct {
	process CustomSpoutFunc
	oc      *streams.OperatorContext
}

func NewCustomSpout(customSpout CustomSpoutFunc) streams.CreateSpoutProcessorFunc {
	return func() streams.SpoutProcessor {
		return &CustomSpout{
			process: customSpout,
		}
	}
}

func (s *CustomSpout) Setup(ctx context.Context, oc *streams.OperatorContext) {
	s.oc = oc
}

func (s *CustomSpout) Process(ctx context.Context) {
	s.process(ctx, s.oc)
}

func (s *CustomSpout) Teardown() {}
