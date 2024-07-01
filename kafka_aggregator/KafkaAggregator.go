package kafka_aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/domain"
	"log"
)

type KafkaAggregator struct {
	reader        *kafka.Reader
	writer        *kafka.Writer
	schemasReader SchemasReader
	cache         FieldsCache
}

func NewKafkaAggregator(kafkaBroker string, mergedSourcesTopic string, schemasReader SchemasReader, cache FieldsCache) *KafkaAggregator {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    mergedSourcesTopic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Balancer: &kafka.LeastBytes{},
	}

	return &KafkaAggregator{
		reader:        reader,
		writer:        writer,
		schemasReader: schemasReader,
		cache:         cache,
	}
}

func (ka *KafkaAggregator) Listen(ctx context.Context) {
	log.Printf("[KafkaAggregator] started")
	for {
		msg, err := ka.reader.ReadMessage(ctx)
		log.Printf("[KafkaAggregator] Reading message %s", string(msg.Value))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			errMsg := fmt.Sprintf("[KafkaAggregator] could not read message %v", err)
			panic(errMsg)
		}
		id := getIdFromMessage(msg)
		log.Printf("[KafkaAggregator] aggregation key %s", id) // fixme
		if err = ka.WriteAggregate(id, msg, ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			errMsg := fmt.Sprintf("[KafkaAggregator] could not write message %v", err)
			panic(errMsg)
		}
		log.Printf("[KafkaAggregator] aggregation for %s done", id) // fixme
	}
}

// fixme: we don't know which is the message key, ideally it should be some id, e.g. user_id
// anyway, this id can be configurable for each topic
func getIdFromMessage(m kafka.Message) string {
	var temp map[string]any
	err := json.Unmarshal(m.Key, &temp)
	log.Printf("[KafkaAggregator] message key %s", string(m.Key))
	if err != nil {
		errMsg := fmt.Sprintf("[KafkaAggregator] could not unmarshal message %v", err)
		panic(errMsg)
	}
	if len(temp) == 0 {
		panic("[KafkaAggregator] aggregation key is missing")
	}
	if len(temp) > 1 {
		panic("[KafkaAggregator] aggregation key is not a single string")
	}
	var key string
	for _, v := range temp {
		key = v.(string) // fixme should be safe
		break
	}
	return key
}

func (ka *KafkaAggregator) toMap(data []byte) map[string]any {
	var result map[string]any
	err := json.Unmarshal(data, &result)
	if err != nil {
		log.Printf("[KafkaAggregator] could not unmarshal message %v", err) // fixme fatal?
	}
	return result
}

func (ka *KafkaAggregator) getSchemaFields(schema domain.Schema) (fields []string, err error) {
	res := make(map[string]any)
	err = json.Unmarshal([]byte(schema.Schema()), &res)
	if err != nil {
		return []string{}, fmt.Errorf("[KafkaAggregator] could not unmarshal schema %v", err)
	}
	fields = make([]string, 0, len(res))
	for k, _ := range res {
		fields = append(fields, k)
	}
	return fields, nil
}

func (ka *KafkaAggregator) composeMessageForSchema(id string, schema domain.Schema, ctx context.Context) kafka.Message {
	res := make(map[string]any)
	fields := schema.Fields()
	subject := schema.Subject()
	for _, key := range fields {
		value, err := ka.cache.Get(id, key, ctx)
		if err != nil {
			log.Printf("[KafkaAggregator] could not get value for key %s, %v", key, err)
		}
		res[key] = value
	}

	payload, err := json.Marshal(res)
	if err != nil {
		log.Printf("[KafkaAggregator] could not marshal message %v", err)
	}
	log.Printf("payload %s", string(payload)) // fixme

	return kafka.Message{
		Topic: subject,
		Key:   []byte(id),
		Value: payload,
	}
}

// todo how id for the message is propagated?
func (ka *KafkaAggregator) WriteAggregate(id string, m kafka.Message, ctx context.Context) error {
	// Get all keys from the json
	log.Printf("[KafkaAggregator] Writing aggregate for id %s", id) // fixme
	kvs := ka.toMap(m.Value)
	fields := make([]string, 0, len(kvs))
	for k := range kvs {
		fields = append(fields, k)
	}
	log.Printf("[KafkaAggregator] fields to aggregate: %v", fields) // fixme

	errorsAll := make([]error, 0, len(fields))
	// Upsert schemaswriter
	for k, v := range kvs {
		err := ka.cache.Upsert(id, k, v, ctx) // !!! fixme we should upsert only fields presented in a schema
		if err != nil {
			log.Printf("[KafkaAggregator] could not update schemaswriter for id %s, key %s, value %s, error: %v", id, k, v, err)
			errorsAll = append(errorsAll, err)
		}
	}

	// Get all schemas for each field
	field2Schemas := make(map[string][]domain.Schema)
	for _, field := range fields {
		schemas, err := ka.schemasReader.GetSchemasForField(field, ctx)
		if err != nil {
			log.Printf("[KafkaAggregator] could not get schemas for field %s, %v", field, err)
			errorsAll = append(errorsAll, err)
		}
		field2Schemas[field] = schemas
	}
	log.Printf("[KafkaAggregator] field2Schemas: %v", field2Schemas) // fixme

	schemaMap := make(map[string]domain.Schema)
	for _, schemas := range field2Schemas {
		for _, schema := range schemas {
			schemaMap[schema.Key()] = schema
		}
	}
	log.Printf("schemaMap: %v", schemaMap) // fixme
	// write kafka records for each schema
	// todo it's better to ensure that we write only in case there's un update for at least one field of the schema
	for _, schema := range schemaMap {
		var msg kafka.Message
		msg = ka.composeMessageForSchema(id, schema, ctx)
		err := ka.writer.WriteMessages(ctx, msg)
		if err != nil {
			log.Printf("[KafkaAggregator] could not write message %v", err)
			errorsAll = append(errorsAll, err)
		}
	}

	if len(errorsAll) == 0 {
		return errors.Join(errorsAll...)
	}
	return nil
}

type SchemasReader interface {
	GetSchemasForField(field string, ctx context.Context) ([]domain.Schema, error)
}

// FieldsCache is a plain storage from id to key-value pairs (e.g. userId -> {k0 -> v0, k1 -> v1, ...})
type FieldsCache interface {
	Get(id string, key string, ctx context.Context) (any, error) // todo should id be any?
	Upsert(id string, key string, value any, ctx context.Context) error
}
