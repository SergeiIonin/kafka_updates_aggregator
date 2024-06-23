package handler

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/domain"
	"log"
)

type KafkaSchemasHandler struct {
	*kafka.Reader
	SchemasWriter
	*KeyTypes
}

func NewKafkaSchemasHandler(kafkaReader *kafka.Reader, writer SchemasWriter) *KafkaSchemasHandler {
	return &KafkaSchemasHandler{
		Reader:        kafkaReader,
		SchemasWriter: writer,
		KeyTypes:      NewKeyTypes(),
	}
}

func (ksh *KafkaSchemasHandler) Run(ctx context.Context) {
	for {
		msg, err := ksh.Reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			return
		}
		keytype, err := ksh.GetKeytype(msg.Key)
		if err != nil {
			log.Printf("%v", err)
			continue
		}
		switch keytype {
		case ksh.SCHEMA:
			var schemaMsg SchemaMsg
			err := json.Unmarshal(msg.Value, &schemaMsg)
			if err != nil {
				log.Printf("Error unmarshalling schemaMsg: %v", err)
				continue
			}
			id, err := ksh.SaveSchema(schemaMsg)
			if err != nil {
				log.Printf("Error saving schema: %v", err)
				continue
			}
			log.Printf("Saved schema with id: %v", id)
		case ksh.DELETE_SUBJECT:
			var deleteSubjectMsg DeleteSubjectMsg
			err := json.Unmarshal(msg.Value, &deleteSubjectMsg)
			if err != nil {
				log.Printf("Error unmarshalling deleteSubjectMsg: %v", err)
				continue
			}
			id, err := ksh.DeleteSchema(deleteSubjectMsg)
			if err != nil {
				log.Printf("Error deleting schema: %v", err)
				continue
			}
			log.Printf("Deleted schema with id: %v", id)
		}
	}
}

func getSchemaFromSchemaMsg(schemaMsg SchemaMsg) (domain.Schema, error) {
	var schemaInternal SchemaInternal
	err := json.Unmarshal([]byte(schemaMsg.Schema), &schemaInternal)
	if err != nil {
		log.Printf("Error unmarshalling SchemaMsg: %v", err)
		return domain.Schema{}, err
	}
	fields := make([]string, 0, len(schemaInternal.Fields))
	for _, f := range schemaInternal.Fields {
		fields = append(fields, f.Name)
	}
	return domain.Schema{
		Subject: schemaMsg.Subject,
		Version: schemaMsg.Version,
		ID:      schemaMsg.ID,
		Fields:  fields,
		Schema:  schemaMsg.Schema,
	}, nil
}

func (ksh *KafkaSchemasHandler) SaveSchema(msg SchemaMsg) (string, error) {
	schema, err := getSchemaFromSchemaMsg(msg)
	if err != nil {
		log.Printf("Error getting schema from schemaMsg: %v", err)
		return "", err
	}
	return ksh.SchemasWriter.SaveSchema(schema)
}

func (ksh *KafkaSchemasHandler) DeleteSchema(msg DeleteSubjectMsg) (string, error) {
	return ksh.SchemasWriter.DeleteSchema(msg.Subject, msg.Version)
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
