package test_kafka_aggregator

import (
	"context"
	"kafka_updates_aggregator/domain"
)

type SchemasReaderTestImpl struct {
	FieldToSchemas map[string][]domain.Schema
}

func NewSchemasReaderTestImpl(underlying map[string][]domain.Schema) SchemasReaderTestImpl {
	return SchemasReaderTestImpl{
		FieldToSchemas: underlying,
	}
}

func (srt SchemasReaderTestImpl) GetSchemasForField(ctx context.Context, field string) ([]domain.Schema, error) {
	return srt.FieldToSchemas[field], nil
}
