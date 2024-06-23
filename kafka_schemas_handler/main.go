package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/kafka_schemas_handler/handler"
)

type SchemasDAOImpl struct {
	Underlying map[string]string
}

func NewSchemasDAOImpl() *SchemasDAOImpl {
	return &SchemasDAOImpl{
		Underlying: make(map[string]string),
	}
}

func getSubjectID(subject string, version int) string {
	return fmt.Sprintf("%s-%d", subject, version)
}

func (dao *SchemasDAOImpl) SaveSchema(subject string, version int, id int, schema string) (string, error) {
	schemaID := getSubjectID(subject, version)
	dao.Underlying[schemaID] = schema
	return schemaID, nil
}

func (dao *SchemasDAOImpl) DeleteSchema(subject string, version int) (string, error) {
	schemaID := getSubjectID(subject, version)
	delete(dao.Underlying, schemaID)
	return schemaID, nil
}

func main() {
	kafkaBrokers := []string{"localhost:19092"}
	//kafkaAddr := kafka.TCP(kafkaBrokers[0])
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafkaBrokers,
		Topic:    "_schemas",
		GroupID:  "schemas_handler", // fixme
		MinBytes: 10e3,              // 10KB
		MaxBytes: 10e6,              // 10MB
	})
	schemasDAO := NewSchemasDAOImpl()
	KafkaSchemasHandler := handler.NewKafkaSchemasHandler(reader, schemasDAO)
	ctx := context.Background()
	KafkaSchemasHandler.Run(ctx)
}
