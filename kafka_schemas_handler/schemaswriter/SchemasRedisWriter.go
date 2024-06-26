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
	fieldPrefix  string
	schemaPrefix string
}

func NewSchemasRedisWriter(redisAddr string) *SchemasRedisWriter {
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &SchemasRedisWriter{redis: redisClient, fieldPrefix: "field.", schemaPrefix: "schema."}
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
	schemaRedisKey := fmt.Sprintf("%s%s", srw.schemaPrefix, schema.Key())
	fieldsJson, err := json.Marshal(schema.Fields())
	if err != nil {
		log.Printf("Error marshalling fields for schema %s to json: %v", schemaRedisKey, err)
		return err
	}
	if err := srw.redis.Set(context.Background(), schemaRedisKey, string(fieldsJson), 0).Err(); err != nil {
		log.Printf("Error setting schema schemaRedisKey in redis: %v", err)
		return err
	}
	return nil
}

func (srw *SchemasRedisWriter) addSchemaForField(ctx context.Context, schema domain.Schema, field string) error {
	fieldRedisKey := fmt.Sprintf("%s%s", srw.fieldPrefix, field)

	schemaRaw, err := json.Marshal(&schema)
	if err != nil {
		log.Printf("Error marshalling schema for field %s to json: %v", fieldRedisKey, err)
		return err
	}

	if err = srw.redis.HSet(ctx, fieldRedisKey, schema.Key(), schemaRaw).Err(); err != nil {
		log.Printf("Error saving new schema for field -> schema in redis: %v", err)
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
	}

	msg := fmt.Sprintf("schema %s already exists in redis", schemaKey)
	log.Println(msg)
	return schemaKey, errors.New(msg)
}

func (srw *SchemasRedisWriter) DeleteSchema(subject string, version int, ctx context.Context) (string, error) {
	schemaKey := fmt.Sprintf("%s.%d", subject, version)
	schemaRedisKey := fmt.Sprintf("%s%s", srw.schemaPrefix, schemaKey)
	log.Printf("deleting schema %v", schemaKey)

	if srw.redis.Exists(ctx, schemaRedisKey).Val() == 0 {
		msg := fmt.Sprintf("schema %s does not exist in redis", schemaRedisKey)
		log.Println(msg)
		return schemaKey, errors.New(msg)
	}

	fieldsRaw := srw.redis.Get(ctx, schemaRedisKey).Val()
	var fields []string
	err := json.Unmarshal([]byte(fieldsRaw), &fields)
	if err != nil {
		log.Printf("Error unmarshalling fields for schema %s from redis: %v", schemaRedisKey, err)
		return schemaKey, err
	}

	errorsAll := make([]error, 0, len(fields))
	for _, field := range fields {
		fieldRedisKey := fmt.Sprintf("%s%s", srw.fieldPrefix, field)

		err = srw.redis.HDel(ctx, fieldRedisKey, schemaKey).Err()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				log.Printf("Error deleting schema %s for key %s from redis: %v", schemaKey, fieldRedisKey, err)
				errorsAll = append(errorsAll, err)
			}
		}
	}

	if err = srw.redis.Del(ctx, schemaRedisKey).Err(); err != nil {
		errorsAll = append(errorsAll, err)
	}

	if len(errorsAll) == 0 {
		return schemaKey, nil
	} else {
		return "", errors.Join(errorsAll...)
	}

}
