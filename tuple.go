package streams

// var tuplePool sync.Pool

// func init() {
// 	tuplePool = sync.Pool{
// 		New: func() interface{} {
// 			return &Tuple{
// 				Metadata: TupleMetadata{},
// 				Data:     TupleData{},
// 			}
// 		},
// 	}
// }

// func CreateTuple() *Tuple {
// 	return tuplePool.Get().(*Tuple)
// }

// func ReturnTuple(t *Tuple) {
// 	tuplePool.Put(t)
// }

// Tuple is the type of the struct instance that is transmitted between
// Topology operators. It contains Metadata, set by the Topology fabric,
// and Data, which contains user-defined data consumed and produced by
// SourceOperator and Operator instances.
type Tuple struct {
	Metadata TupleMetadata
	Data     TupleData
}

// TupleMetadata is a member of the Tuple struct. It contains metadata about
// the tuple, including the name of the stream it was consumed from and the
// name of the operator that produced the stream.
type TupleMetadata struct {
	StreamName string
	Producer   string
	Instance   int
}

// TupleData is a member of the Tuple struct. It is a map of string keys which
// can take on any user-defined values (via the generic interface{} value type)
type TupleData map[string]interface{}
