package kafka_aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/domain"
	"log"
	"strconv"
	"time"
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

func (ka *KafkaAggregator) Run(ctx context.Context) {
	log.Printf("[KafkaAggregator] started")
	for {
		msg, err := ka.reader.ReadMessage(ctx)
		currentTime := time.Now().UnixMilli()
		log.Printf("[KafkaAggregator] %d Reading message %s", currentTime, string(msg.Value))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			errMsg := fmt.Sprintf("[KafkaAggregator] could not read message %v", err)
			panic(errMsg)
		}
		id := getIdFromMessage(msg)
		log.Printf("[KafkaAggregator] aggregation key %s", id) // fixme
		if err = ka.WriteAggregate(ctx, id, msg); err != nil {
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

func parseFloat32(value string) (float32, error) {
	f, err := strconv.ParseFloat(value, 32)
	if err != nil {
		return 0, err
	}
	return float32(f), nil
}

func getFieldTypedValue(field domain.Field, value string) (any, error) {
	switch field.Type {
	case "string":
		return value, nil
	case "bool":
		return strconv.ParseBool(value)
	case "int":
		return strconv.Atoi(value)
	case "int64":
		return strconv.ParseInt(value, 10, 64)
	case "uint64":
		return strconv.ParseUint(value, 10, 64)
	case "float32":
		return parseFloat32(value)
	case "float64":
		return strconv.ParseFloat(value, 64)
	case "time":
		return time.Parse(time.RFC3339Nano, value)
	default:
		return nil, fmt.Errorf("[KafkaAggregator] could not get value for field %s", field.Name)
	}
}

func (ka *KafkaAggregator) composeMessageForSchema(ctx context.Context, id string, schema domain.Schema) (kafka.Message, error) {
	res := make(map[string]any)
	fields := schema.Fields()
	subject := schema.Subject()
	for _, field := range fields {
		value, err := ka.cache.Get(ctx, id, field.Name)
		if err != nil {
			log.Printf("[KafkaAggregator] could not get value for key %s, %v", field.Name, err)
			panic(err)
		}
		if value == "" {
			return kafka.Message{}, errors.New("no value")
		}
		valueTyped, err := getFieldTypedValue(field, value)
		if err != nil {
			log.Printf("[KafkaAggregator] could not get value for key %s, %v", field.Name, err)
			panic(err)
		}
		res[field.Name] = valueTyped
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
	}, nil
}

// todo how id for the message is propagated?
func (ka *KafkaAggregator) WriteAggregate(ctx context.Context, id string, m kafka.Message) error {
	// Get all keys from the json
	log.Printf("[KafkaAggregator] Writing aggregate for id %s", id) // fixme
	kvs := ka.toMap(m.Value)
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	log.Printf("[KafkaAggregator] keys to aggregate: %v", keys) // fixme

	errorsAll := make([]error, 0, len(keys))

	// Get all schemas for each key
	field2Schemas := make(map[string][]domain.Schema)
	for _, key := range keys {
		schemas, err := ka.schemasReader.GetSchemasForKey(ctx, key)
		if err != nil {
			log.Printf("[KafkaAggregator] could not get schemas for key %s, %v", key, err)
			errorsAll = append(errorsAll, err)
		}
		if len(schemas) != 0 {
			value := kvs[key]
			err = ka.cache.Upsert(ctx, id, key, value)
			if err != nil {
				log.Printf("[KafkaAggregator] could not update schemaswriter for id %s, key %s, value %s, error: %v", id, key, value, err)
				errorsAll = append(errorsAll, err)
			}
		}
		field2Schemas[key] = schemas
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
	// todo it's better to ensure that we write only in case there's un update for at least one key of the schema
	for _, schema := range schemaMap {
		var msg kafka.Message
		msg, err := ka.composeMessageForSchema(ctx, id, schema)
		if errors.Is(err, errors.New("no value")) {
			continue
		}
		err = ka.writer.WriteMessages(ctx, msg)
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
	GetSchemasForKey(ctx context.Context, key string) ([]domain.Schema, error)
}

// FieldsCache is a plain storage from id to key-value pairs (e.g. userId -> {k0 -> v0, k1 -> v1, ...})
type FieldsCache interface {
	Get(ctx context.Context, id string, key string) (string, error) // todo should id be any? can we utilize generics?
	Upsert(ctx context.Context, id string, key string, value any) error
}
