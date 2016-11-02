package operators

import (
	"context"

	"github.com/joelanford/conductor"
)

type CustomSpoutFunc func(context.Context, conductor.OperatorContext)

type CustomSpout struct {
	process CustomSpoutFunc
	oc      conductor.OperatorContext
}

func NewCustomSpout(customSpout CustomSpoutFunc) conductor.CreateSpoutProcessorFunc {
	return func() conductor.SpoutProcessor {
		return &CustomSpout{
			process: customSpout,
		}
	}
}

func (s *CustomSpout) Setup(ctx context.Context, oc conductor.OperatorContext) {
	s.oc = oc
}

func (s *CustomSpout) Process(ctx context.Context) {
	s.process(ctx, s.oc)
}

func (s *CustomSpout) Teardown() {}
