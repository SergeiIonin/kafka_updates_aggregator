package kafka_schemas_handler

import (
	"context"
	"kafka_updates_aggregator/domain"
)

type SchemasWriter interface {
	SaveSchema(ctx context.Context, schema domain.Schema) (string, error)
	DeleteSchema(ctx context.Context, subject string, version int) (string, error)
}
