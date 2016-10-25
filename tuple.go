package conductor

type Tuple struct {
	Metadata *TupleMetadata
	Data     TupleData
}

type TupleMetadata struct {
	StreamName string
	Producer   string
}

type TupleData map[string]interface{}
