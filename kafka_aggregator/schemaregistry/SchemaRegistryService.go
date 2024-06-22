package schemaregistry

import (
	"encoding/json"
	"fmt"
	"github.com/riferrei/srclient"
)

type SchemaRegistryService struct {
	Client srclient.ISchemaRegistryClient
	Repo   SchemaRepo
}

func NewSchemaRegistryService(schemaRegistryUrl string) *SchemaRegistryService {
	client := srclient.CreateSchemaRegistryClient(schemaRegistryUrl)
	return &SchemaRegistryService{
		Client: client,
	}
}

func (srs *SchemaRegistryService) SaveSchema(schema *srclient.Schema) error {
	fields, _ := srs.GetSchemaFields(schema)
	for _, field := range fields {
		if err := srs.Repo.AddSchemaToField(field, schema); err != nil {
			return fmt.Errorf("could not add schema to field %v", err)
		}
	}
	return nil
}

// todo support avro and protobuf as well
// if schema has multiple References then they can be different in only Version (Subject and Name should be the same)
// tests only
func (srs *SchemaRegistryService) GetSchemaSubject(schema *srclient.Schema) (string, error) {
	return srs.Repo.GetSubjectBySchema(schema)
	/*schemaType := schema.SchemaType()
	if *schemaType != srclient.Json {
		return "", fmt.Errorf("unsupported schema type %v", *schemaType)
	}
	subject := schema.References()[0].Subject
	return subject, nil*/
}

func (srs *SchemaRegistryService) GetSchemasForField(field string) ([]*srclient.Schema, error) {
	return srs.Repo.GetSchemasByField(field)
}

type fieldRepr struct {
	Name string
	Type string
}

type schemaRepr struct {
	Type   string
	Name   string
	Fields []fieldRepr
}

func (srs *SchemaRegistryService) GetSchemaFields(schema *srclient.Schema) ([]string, error) {
	schemaType := schema.SchemaType()
	if *schemaType != srclient.Json {
		return nil, fmt.Errorf("unsupported schema type %v", *schemaType)
	}
	res := schemaRepr{}
	err := json.Unmarshal([]byte(schema.Schema()), &res)
	if err != nil {
		return []string{}, fmt.Errorf("could not unmarshal schema %v", err)
	}
	fields := make([]string, 0, len(res.Fields))
	for _, field := range res.Fields {
		fields = append(fields, field.Name)
	}
	return fields, nil
}

type SchemaRepo interface {
	AddSchemaToField(field string, schema *srclient.Schema) error
	GetSchemasByField(field string) ([]*srclient.Schema, error)
	GetSubjectBySchema(schema *srclient.Schema) (string, error)
}
