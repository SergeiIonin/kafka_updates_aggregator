package schemasreader

import (
	"context"
	"encoding/json"
	"errors"
	"kafka_updates_aggregator/cache"
	"kafka_updates_aggregator/domain"

	"github.com/redis/go-redis/v9"

	"log"
)

// SchemasRedisReader is used to read all schemas where the field is present
type SchemasRedisReader struct {
	redis *redis.Client
}

func NewSchemasRedisReader(redisAddr string) *SchemasRedisReader {
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &SchemasRedisReader{redis: redisClient}
}

func (srr *SchemasRedisReader) GetSchemasForKey(ctx context.Context, key string) ([]domain.Schema, error) {
	fieldRedisKey := cache.GetFieldKey(key)

	mapping, err := srr.redis.HGetAll(ctx, fieldRedisKey).Result()
	if err != nil {
		log.Printf("Error getting schemas for key %s: %s", fieldRedisKey, err)
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
