package kafka_schemas_handler

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/domain"
	"log"
)

type KafkaSchemasHandler struct {
	kafkaReader   *kafka.Reader
	schemasWriter SchemasWriter
	*KeyTypes
}

func NewKafkaSchemasHandler(kafkaBroker string, writer SchemasWriter) *KafkaSchemasHandler {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    "_schemas",
		GroupID:  "schemas_handler",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	return &KafkaSchemasHandler{
		kafkaReader:   kafkaReader,
		schemasWriter: writer,
		KeyTypes:      NewKeyTypes(),
	}
}

func (ksh *KafkaSchemasHandler) Run(ctx context.Context) {
	log.Printf("[KafkaSchemasHandler] Started")
	for {
		msg, err := ksh.kafkaReader.ReadMessage(ctx)
		if err != nil {
			log.Printf("[KafkaSchemasHandler] Error reading message: %v", err)
			return
		}
		log.Printf("[KafkaSchemasHandler] Received message with key: %v", string(msg.Key))
		keytype, err := ksh.GetKeytype(msg.Key)
		if err != nil {
			log.Printf("[KafkaSchemasHandler] error retrieving message key: %v", err)
			continue
		}
		switch keytype {
		case ksh.SCHEMA:
			var schemaMsg SchemaMsg
			err := json.Unmarshal(msg.Value, &schemaMsg)
			log.Printf("[KafkaSchemasHandler] received new schema: %v", schemaMsg)
			if err != nil {
				log.Printf("[KafkaSchemasHandler] Error unmarshalling schemaMsg: %v", err)
				continue
			}
			id, err := ksh.SaveSchema(ctx, schemaMsg)
			if err != nil {
				log.Printf("[KafkaSchemasHandler] Error saving schema: %v", err)
				continue
			}
			log.Printf("[KafkaSchemasHandler] Saved schema with id: %v", id)
		case ksh.DELETE_SUBJECT:
			var deleteSubjectMsg DeleteSubjectMsg
			err := json.Unmarshal(msg.Value, &deleteSubjectMsg)
			log.Printf("[KafkaSchemasHandler] request to delete schema: %v", deleteSubjectMsg)
			if err != nil {
				log.Printf("[KafkaSchemasHandler] Error unmarshalling deleteSubjectMsg: %v", err)
				continue
			}
			id, err := ksh.DeleteSchema(ctx, deleteSubjectMsg)
			if err != nil {
				log.Printf("[KafkaSchemasHandler] Error deleting schema: %v", err)
				continue
			}
			log.Printf("[KafkaSchemasHandler] Deleted schema with id: %v", id)
		}
	}
}

func getSchemaFromSchemaMsg(schemaMsg SchemaMsg) (domain.Schema, error) {
	var schemaInternal SchemaInternal
	err := json.Unmarshal([]byte(schemaMsg.Schema), &schemaInternal)
	if err != nil {
		log.Printf("[KafkaSchemasHandler] Error unmarshalling SchemaMsg: %v", err)
		return domain.Schema{}, err
	}
	fields := make([]domain.Field, 0, len(schemaInternal.Fields))
	for _, f := range schemaInternal.Fields {
		fields = append(fields, domain.Field{f.Name, f.Type})
	}
	return *domain.CreateSchema(schemaMsg.Subject, schemaMsg.Version, schemaMsg.ID, fields, schemaMsg.Schema), nil
}

func (ksh *KafkaSchemasHandler) SaveSchema(ctx context.Context, msg SchemaMsg) (string, error) {
	schema, err := getSchemaFromSchemaMsg(msg)
	if err != nil {
		log.Printf("[KafkaSchemasHandler] Error getting schema from schemaMsg: %v", err)
		return "", err
	}
	return ksh.schemasWriter.SaveSchema(ctx, schema)
}

func (ksh *KafkaSchemasHandler) DeleteSchema(ctx context.Context, msg DeleteSubjectMsg) (string, error) {
	return ksh.schemasWriter.DeleteSchema(ctx, msg.Subject, msg.Version)
}

type SchemaMsg struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

type SchemaInternal struct {
	Type   string  `json:"type"`
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type DeleteSubjectMsg struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}
