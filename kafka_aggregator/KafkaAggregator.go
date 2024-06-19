package kafka_aggregator

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
)

type KafkaAggregator struct {
	kafka.Reader
	kafka.Writer
	SchemaService
	Cache
}

func (ka *KafkaAggregator) Listen() {
	// todo implement
}

func (ka *KafkaAggregator) toMap(data []byte) map[string]interface{} {
	var result map[string]interface{}
	err := json.Unmarshal(data, &result)
	if err != nil {
		log.Printf("could not unmarshal message %v", err) // fixme fatal?
	}
	return result
}

func (ka *KafkaAggregator) ComposeMessageForSchema(id string, schema Schema) kafka.Message {
	res := make(map[string]string)
	for _, key := range schema.Keys {
		value, err := ka.Get(schema.Namespace, id, key)
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
		Topic: schema.Topic,
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

	// Get field -> namespaces map
	fields2ns := make(map[string][]string)
	for _, field := range fields {
		namespaces, err := ka.GetNamespacesForField(field)
		if err != nil {
			log.Printf("could not get namespaces for field %s, %v", field, err)
		}
		fields2ns[field] = namespaces
	}

	// Update cache
	for k, nss := range fields2ns {
		for _, ns := range nss {
			ka.Update(ns, k, kvs[k].(string))
		}
	}

	// Get all schemas for each field
	field2Schemas := make(map[string][]Schema)
	for _, field := range fields {
		schemas, err := ka.GetSchemasForField(field)
		if err != nil {
			log.Printf("could not get schemas for field %s, %v", field, err)
		}
		field2Schemas[field] = schemas
	}

	ctx := context.Background()
	// write kafka records for each schema
	// todo it's better to ensure that we write only in case there's un update for at least one field of the schema
	for _, schemas := range field2Schemas {
		var msg kafka.Message
		for _, schema := range schemas {
			msg = ka.ComposeMessageForSchema(id, schema)
			ka.WriteMessages(ctx, msg)
		}
	}

}

// fixme temp struct
type Schema struct {
	Namespace string
	Topic     string
	Keys      []string
}

type SchemaService interface {
	// returns a json/avro schemas for a given field
	GetSchemasForNamespace(ns string) ([]Schema, error)
	GetSchemasForField(field string) ([]Schema, error)
	GetNamespacesForField(field string) ([]string, error)
}

// Cache has a namespace (e.g. `users`, `items`) which has an id. Within the namespace
// KV pairs associated with the namespace are stored per each id.
// E.g. for the `users` ns we could have `users:1` -> `{"name": "John", "age": 25, "balance": 1000, "last_deposit": 500}` etc
// Above ns = users, id = 1, key = name/age/balance/last_deposit, value = John/25/1000/500
type Cache interface {
	CreateNamespace(ns string) error
	DeleteNamespace(ns string) error
	Create(ns string, id string, key string, value string) error
	Get(ns string, id string, key string) (string, error)
	Update(ns string, key string, value string) error
	Delete(ns string, key string) error
}
