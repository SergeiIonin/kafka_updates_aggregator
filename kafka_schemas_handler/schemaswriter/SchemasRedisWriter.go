package schemaswriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"kafka_updates_aggregator/domain"
	"log"
)

type SchemasRedisWriter struct {
	redis        *redis.Client
	hsetName     string
	fieldPrefix  string
	schemaPrefix string
}

func NewSchemasRedisWriter(redisAddr string) *SchemasRedisWriter {
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &SchemasRedisWriter{redis: redisClient, hsetName: "schemas", fieldPrefix: "field.", schemaPrefix: "schema."}
}

func readSchemas(rawSchemas []byte) ([]domain.Schema, error) {
	if len(rawSchemas) == 0 {
		return []domain.Schema{}, nil
	}
	var schemas []domain.Schema
	err := json.Unmarshal(rawSchemas, &schemas)
	if err != nil {
		log.Printf("error unmarshalling schemas from redis: %v", err)
		return nil, err
	}
	return schemas, nil
}

func (srw *SchemasRedisWriter) containsSchema(schema domain.Schema) bool {
	key := fmt.Sprintf("%s%s", srw.schemaPrefix, schema.Key())
	return srw.redis.Exists(context.Background(), key).Val() == 1
}

func (srw *SchemasRedisWriter) addSchemaKey(schema domain.Schema) error {
	key := fmt.Sprintf("%s%s", srw.schemaPrefix, schema.Key())
	if err := srw.redis.Set(context.Background(), key, true, 0).Err(); err != nil {
		log.Printf("Error setting schema key in redis: %v", err)
		return err
	}
	return nil
}

func (srw *SchemasRedisWriter) addSchemaForField(ctx context.Context, schema domain.Schema, field string) error {
	fieldRedisKey := fmt.Sprintf("%s%s", srw.fieldPrefix, field)

	schemasRaw, err := srw.redis.HGet(ctx, fieldRedisKey, srw.hsetName).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Printf("Error fetching schemas for field %s from redis: %v", fieldRedisKey, err)
			return err
		}
	}
	schemas, err := readSchemas([]byte(schemasRaw))
	if err != nil {
		return err
	}

	schemasUpd := append(schemas, schema)

	schemasRawUpd, err := json.Marshal(schemasUpd)
	if err != nil {
		log.Printf("Error marshalling schemas for field %s to json: %v", fieldRedisKey, err)
		return err
	}

	if err = srw.redis.HSet(ctx, fieldRedisKey, srw.hsetName, schemasRawUpd).Err(); err != nil {
		log.Printf("Error saving new schema for field -> []schema in redis: %v", err)
		return err
	}
	return nil
}

func (srw *SchemasRedisWriter) SaveSchema(schema domain.Schema, ctx context.Context) (string, error) {
	log.Printf("saving schema %v", schema)
	schemaKey := schema.Key()
	if !srw.containsSchema(schema) {
		if err := srw.addSchemaKey(schema); err != nil {
			return "", err
		}
		fields := schema.Fields()
		errorsAll := make([]error, 0, len(fields))
		for _, field := range fields {
			err := srw.addSchemaForField(ctx, schema, field)
			if err != nil {
				errorsAll = append(errorsAll, err)
			}
		}
		if len(errorsAll) == 0 {
			return schemaKey, nil
		} else {
			return "", errors.Join(errorsAll...)
		}
	} else {
		msg := fmt.Sprintf("schema %s already exists in redis", schemaKey)
		log.Println(msg)
		return schemaKey, errors.New(msg)
	}
}

func (srw *SchemasRedisWriter) DeleteSchema(subject string, version int, ctx context.Context) (string, error) {
	return "", nil
}
