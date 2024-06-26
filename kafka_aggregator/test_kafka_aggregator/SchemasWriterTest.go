package test_kafka_aggregator

import (
	"kafka_updates_aggregator/domain"
)

type SchemasWriterTest struct {
	FieldToSchemas map[string][]domain.Schema
}

func NewSchemasWriterTest(underlying map[string][]domain.Schema) SchemasWriterTest {
	return SchemasWriterTest{
		FieldToSchemas: underlying,
	}
}
func (srt SchemasWriterTest) AddSchemaToField(field string, schema domain.Schema) {
	srt.FieldToSchemas[field] = append(srt.FieldToSchemas[field], schema)
}
