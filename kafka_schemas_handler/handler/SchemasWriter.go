package handler

import "kafka_updates_aggregator/domain"

type SchemasWriter interface {
	SaveSchema(schema domain.Schema) (string, error) // fixme should accept Schema
	DeleteSchema(subject string, version int) (string, error)
}
