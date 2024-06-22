package kafka_aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"
	"log"
)

type KafkaAggregator struct {
	*kafka.Reader
	*kafka.Writer
	SchemaService
	Cache
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

func (ka *KafkaAggregator) getSchemaFields(schema srclient.Schema) (fields []string, err error) {
	schemaType := schema.SchemaType()
	if *schemaType != srclient.Json {
		return nil, fmt.Errorf("unsupported schema type %v", *schemaType)
	}
	res := make(map[string]any)
	err = json.Unmarshal([]byte(schema.Schema()), &res)
	if err != nil {
		return []string{}, fmt.Errorf("could not unmarshal schema %v", err)
	}
	fields = make([]string, 0, len(res))
	for k, _ := range res {
		fields = append(fields, k)
	}
	return fields, nil
}

func (ka *KafkaAggregator) ComposeMessageForSchema(id string, schema *srclient.Schema) kafka.Message {
	res := make(map[string]any)
	fields, err := ka.GetSchemaFields(schema)
	if err != nil {
		log.Fatalf("could not get fields for schema %v", err)
	}
	subject, err := ka.GetSchemaSubject(schema)
	if err != nil {
		log.Fatalf("could not get topic for schema %v", err)
	}
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
	field2Schemas := make(map[string][]*srclient.Schema)
	for _, field := range fields {
		schemas, err := ka.GetSchemasForField(field)
		if err != nil {
			log.Printf("could not get schemas for field %s, %v", field, err)
		}
		field2Schemas[field] = schemas
	}

	schemaMap := make(map[int]*srclient.Schema)
	for _, schemas := range field2Schemas {
		for _, schema := range schemas {
			schemaMap[schema.ID()] = schema
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

// Note that schema's subject is the same as schema's topic (which is the topic w/ the aggregated info, NOT any topic existed initially)
type SchemaService interface {
	SaveSchema(schema *srclient.Schema) error
	GetSchemaSubject(schema *srclient.Schema) (subject string, err error)
	// returns a json/avro/protobuf schemas for a given field
	GetSchemasForField(field string) ([]*srclient.Schema, error)
	GetSchemaFields(schema *srclient.Schema) ([]string, error)
}

// Cache is a plain storage from id to key-value pairs (e.g. userId -> {k0 -> v0, k1 -> v1, ...})
type Cache interface {
	Get(id string, key string) (any, error) // todo should id be any?
	Update(id string, key string, value any) error
}
