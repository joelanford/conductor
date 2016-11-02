package operators

import (
	"context"

	"time"

	probably "github.com/dustin/go-probably"
	"github.com/joelanford/conductor"
)

type TopN struct {
	oc    conductor.OperatorContext
	field string
	top   *probably.StreamTop
}

func NewTopN(w, d, max int, field string) conductor.CreateBoltProcessorFunc {
	return func() conductor.BoltProcessor {
		return &TopN{
			top:   probably.NewStreamTop(w, d, max),
			field: field,
		}
	}
}

func (b *TopN) Setup(ctx context.Context, oc conductor.OperatorContext) {
	b.oc = oc
	go func() {
		t := time.NewTicker(time.Second * 2)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				oc.Submit(conductor.TupleData{"topn": b.top.GetTop()}, 0)
			}
		}
	}()
}
func (b *TopN) Process(ctx context.Context, t conductor.Tuple, port int) {
	if value, ok := t.Data[b.field].(string); ok {
		b.top.Add(value)
	}
}
func (b *TopN) Teardown() {}
