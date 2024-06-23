package kafka_aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/domain"
	"log"
)

type KafkaAggregator struct {
	*kafka.Reader
	*kafka.Writer
	SchemasReader
	FieldsCache
}

func (ka *KafkaAggregator) Listen() {
	// todo implement
}

func (ka *KafkaAggregator) toMap(data []byte) map[string]any {
	var result map[string]any
	err := json.Unmarshal(data, &result)
	if err != nil {
		log.Printf("could not unmarshal message %v", err) // fixme fatal?
	}
	return result
}

func (ka *KafkaAggregator) getSchemaFields(schema domain.Schema) (fields []string, err error) {
	res := make(map[string]any)
	err = json.Unmarshal([]byte(schema.Schema), &res)
	if err != nil {
		return []string{}, fmt.Errorf("could not unmarshal schema %v", err)
	}
	fields = make([]string, 0, len(res))
	for k, _ := range res {
		fields = append(fields, k)
	}
	return fields, nil
}

func (ka *KafkaAggregator) ComposeMessageForSchema(id string, schema domain.Schema) kafka.Message {
	res := make(map[string]any)
	fields := schema.Fields
	subject := schema.Subject
	for _, key := range fields {
		value, err := ka.Get(id, key)
		if err != nil {
			log.Printf("could not get value for key %s, %v", key, err)
		}
		res[key] = value
	}

	payload, err := json.Marshal(res)
	if err != nil {
		log.Printf("could not marshal message %v", err)
	}

	return kafka.Message{
		Topic: subject,
		Key:   []byte(id),
		Value: payload,
	}
}

// todo how id for the message is propagated?
func (ka *KafkaAggregator) WriteAggregate(id string, m kafka.Message) {
	// Get all keys from the json
	kvs := ka.toMap(m.Value)
	fields := make([]string, 0, len(kvs))
	for k, _ := range kvs {
		fields = append(fields, k)
	}

	// Update cache
	for k, v := range kvs {
		err := ka.Update(id, k, v)
		if err != nil {
			log.Printf("could not update cache for id %s, key %s, value %s, error: %v", id, k, v, err)
		}
	}

	// Get all schemas for each field
	field2Schemas := make(map[string][]domain.Schema)
	for _, field := range fields {
		schemas, err := ka.GetSchemasForField(field)
		if err != nil {
			log.Printf("could not get schemas for field %s, %v", field, err)
		}
		field2Schemas[field] = schemas
	}

	schemaMap := make(map[int]domain.Schema)
	for _, schemas := range field2Schemas {
		for _, schema := range schemas {
			schemaMap[schema.ID] = schema
		}
	}
	ctx := context.Background()
	// write kafka records for each schema
	// todo it's better to ensure that we write only in case there's un update for at least one field of the schema
	for _, schema := range schemaMap {
		var msg kafka.Message
		msg = ka.ComposeMessageForSchema(id, schema)
		err := ka.WriteMessages(ctx, msg)
		if err != nil {
			log.Printf("could not write message %v", err)
		}
	}
}

type SchemasReader interface {
	GetSchemasForField(field string) ([]domain.Schema, error)
}

// FieldsCache is a plain storage from id to key-value pairs (e.g. userId -> {k0 -> v0, k1 -> v1, ...})
type FieldsCache interface {
	Get(id string, key string) (any, error) // todo should id be any?
	Update(id string, key string, value any) error
}
