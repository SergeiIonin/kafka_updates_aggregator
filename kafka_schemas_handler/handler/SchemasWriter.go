package handler

import (
	"context"
	"kafka_updates_aggregator/domain"
)

type SchemasWriter interface {
	SaveSchema(schema domain.Schema, ctx context.Context) (string, error) // fixme should accept Schema
	DeleteSchema(subject string, version int, ctx context.Context) (string, error)
}
