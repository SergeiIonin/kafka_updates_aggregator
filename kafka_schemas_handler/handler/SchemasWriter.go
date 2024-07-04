package handler

import (
	"context"
	"kafka_updates_aggregator/domain"
)

type SchemasWriter interface {
	SaveSchema(ctx context.Context, schema domain.Schema) (string, error) // fixme should accept Schema
	DeleteSchema(ctx context.Context, subject string, version int) (string, error)
}
