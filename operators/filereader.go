package operators

import (
	"context"
	"io/ioutil"

	"github.com/joelanford/conductor"
)

type FileReader struct {
	oc conductor.OperatorContext
}

func NewFileReader() conductor.CreateBoltProcessorFunc {
	return func() conductor.BoltProcessor {
		return &FileReader{}
	}
}

func (b *FileReader) Setup(ctx context.Context, oc conductor.OperatorContext) {
	b.oc = oc
}
func (b *FileReader) Process(ctx context.Context, t conductor.Tuple, port int) {
	name := t.Data["name"].(string)
	operation := t.Data["operation"].(string)

	if operation == "CREATE" || operation == "WRITE" {
		if data, err := ioutil.ReadFile(name); err != nil {
			b.oc.Log().Infof("%s error: %s", b.oc.Name(), err)
		} else {
			b.oc.Submit(conductor.TupleData{"name": name, "operation": operation, "data": data}, 0)
		}
	} else {
		b.oc.Submit(t.Data, 0)
	}
}
func (b *FileReader) Teardown() {}
