package infra

import (
	"kafka_updates_aggregator/restapi/domain"
	"log"

	"github.com/riferrei/srclient"
)

type SchemaRegistryClientImpl struct {
	client *srclient.SchemaRegistryClient
}

func NewSchemaRegistryClientImpl(client *srclient.SchemaRegistryClient) *SchemaRegistryClientImpl {
	return &SchemaRegistryClientImpl{
		client: client,
	}
}

func (c *SchemaRegistryClientImpl) CreateSchema(subject string, schemaRaw string) (int, error) {
	schema, err := c.client.CreateSchema(subject, schemaRaw, srclient.Avro)
	if err != nil {
		log.Printf("Error creating schema: %v", err)
		return -1, err
	}
	return schema.ID(), nil
}
func (c *SchemaRegistryClientImpl) GetSchemas() ([]domain.SchemaRawWithSubject, error) {
	subjects, err := c.client.GetSubjects()
	if err != nil {
		log.Printf("Error getting subjects: %v", err)
		return nil, err
	}
	schemasWithSubjects := make([]domain.SchemaRawWithSubject, 0, len(subjects))
	for _, subject := range subjects {
		schema, err := c.client.GetLatestSchema(subject)
		if err != nil {
			log.Printf("Error getting latest schema: %v", err)
			return nil, err
		}
		schemaWithSubject := domain.SchemaRawWithSubject{
			Subject: subject,
			Schema:  schema.Schema(),
		}
		schemasWithSubjects = append(schemasWithSubjects, schemaWithSubject)
	}
	return schemasWithSubjects, nil
}
