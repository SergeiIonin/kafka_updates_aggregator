package redisprefixes

type RedisPrefixes struct {
	FieldPrefix  string
	SchemaPrefix string
}

func NewRedisPrefixes() *RedisPrefixes {
	return &RedisPrefixes{
		FieldPrefix:  "field.",
		SchemaPrefix: "schema.",
	}
}
