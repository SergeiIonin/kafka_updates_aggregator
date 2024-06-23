package handler

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
)

type KafkaSchemasHandler struct {
	*kafka.Reader
	SchemasDAO
	*KeyTypes
}

func NewKafkaSchemasHandler(reader *kafka.Reader, writer SchemasDAO) *KafkaSchemasHandler {
	return &KafkaSchemasHandler{
		Reader:     reader,
		SchemasDAO: writer,
		KeyTypes:   NewKeyTypes(),
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
			id, err := ksh.SaveSchema(msg)
			if err != nil {
				log.Printf("Error saving schema: %v", err)
				continue
			}
			log.Printf("Saved schema with id: %v", id)
		case ksh.DELETE_SUBJECT:
			id, err := ksh.DeleteSchema(msg)
			if err != nil {
				log.Printf("Error deleting schema: %v", err)
				continue
			}
			log.Printf("Deleted schema with id: %v", id)
		}
	}
}

func (ksh *KafkaSchemasHandler) SaveSchema(msg kafka.Message) (string, error) {
	var schemaMsg SchemaMsg
	err := json.Unmarshal(msg.Value, &schemaMsg)
	if err != nil {
		log.Printf("Error unmarshalling schemaMsg: %v", err)
		return "", err
	}
	return ksh.SchemasDAO.SaveSchema(schemaMsg.Subject, schemaMsg.Version, schemaMsg.ID, schemaMsg.Schema)
}

func (ksh *KafkaSchemasHandler) DeleteSchema(msg kafka.Message) (string, error) {
	var deleteSubjectMsg DeleteSubjectMsg
	err := json.Unmarshal(msg.Value, &deleteSubjectMsg)
	if err != nil {
		log.Printf("Error unmarshalling deleteSubjectMsg: %v", err)
		return "", err
	}
	return ksh.SchemasDAO.DeleteSchema(deleteSubjectMsg.Subject, deleteSubjectMsg.Version)
}

type SchemasDAO interface {
	SaveSchema(subject string, version int, id int, schema string) (string, error)
	DeleteSchema(subject string, version int) (string, error)
}

type SchemaMsg struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

type DeleteSubjectMsg struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}
