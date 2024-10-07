package cache

import (
	"fmt"
	"kafka_updates_aggregator/domain"
)

func GetFieldKey(field string) string {
	return fmt.Sprintf("%s%s", domain.FieldPrefix, field)
}

func GetSchemaKey(key string) string {
	return fmt.Sprintf("%s%s", domain.SchemaPrefix, key)
}
