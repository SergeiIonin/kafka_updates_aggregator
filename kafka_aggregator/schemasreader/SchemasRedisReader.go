package schemasreader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"kafka_updates_aggregator/domain"
	"log"
)

type SchemasRedisReader struct {
	redis        *redis.Client
	fieldPrefix  string // "field."
	schemaPrefix string // "schema."
}

func NewSchemasRedisReader(redisAddr string, fieldPrefix string, schemaPrefix string) *SchemasRedisReader {
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &SchemasRedisReader{redis: redisClient, fieldPrefix: fieldPrefix, schemaPrefix: schemaPrefix}
}

func (srr *SchemasRedisReader) GetSchemasForField(ctx context.Context, field string) ([]domain.Schema, error) {
	fieldRedisKey := fmt.Sprintf("%s%s", srr.fieldPrefix, field)

	mapping, err := srr.redis.HGetAll(ctx, fieldRedisKey).Result()
	if err != nil {
		log.Printf("Error getting schemas for field %s: %s", fieldRedisKey, err)
		return nil, err
	}

	schemas := make([]domain.Schema, 0, len(mapping))
	errorsAll := make([]error, 0, len(mapping))
	var schema domain.Schema
	for _, schemaRaw := range mapping {
		err = json.Unmarshal([]byte(schemaRaw), &schema)
		if err != nil {
			errorsAll = append(errorsAll, err)
			continue
		}
		schemas = append(schemas, schema)
	}

	if len(errorsAll) > 0 {
		return nil, errors.Join(errorsAll...)
	}
	return schemas, nil
}
