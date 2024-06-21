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

func (srs *SchemaRegistryService) SaveSchema(schema *srclient.Schema, namespace string) error {
	fields, _ := srs.GetSchemaFields(schema)
	subject, err := srs.GetSchemaSubject(schema)
	if err != nil {
		return fmt.Errorf("could not get subject for schema %v", err)
	}
	for _, field := range fields {
		if err = srs.Repo.AddSchemaToField(field, schema); err != nil {
			return fmt.Errorf("could not add schema to field %v", err)
		}
		if err = srs.Repo.AddNamespaceToField(field, namespace); err != nil {
			return fmt.Errorf("could not add namespace to field %v", err)
		}
		if err = srs.Repo.AddSubjectToNamespace(subject, namespace); err != nil {
			return fmt.Errorf("could not add subject to namespace %v", err)
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

type FieldRepr struct {
	Name string
	Type string
}

type SchemaRepr struct {
	Type   string
	Name   string
	Fields []FieldRepr
}

func (srs *SchemaRegistryService) GetSchemaFields(schema *srclient.Schema) ([]string, error) {
	schemaType := schema.SchemaType()
	if *schemaType != srclient.Json {
		return nil, fmt.Errorf("unsupported schema type %v", *schemaType)
	}
	res := SchemaRepr{}
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

func (srs *SchemaRegistryService) GetNamespacesForField(field string) ([]string, error) {
	return srs.Repo.GetNamespacesByField(field)
}

func (srs *SchemaRegistryService) GetNamespaceBySubject(subject string) (string, error) {
	return srs.Repo.GetNamespaceBySubject(subject)
}

type SchemaRepo interface {
	AddSchemaToField(field string, schema *srclient.Schema) error
	AddNamespaceToField(field string, namespace string) error
	AddSubjectToNamespace(subject string, namespace string) error
	GetSchemasByField(field string) ([]*srclient.Schema, error)
	GetNamespacesByField(field string) ([]string, error)
	GetNamespaceBySubject(subject string) (string, error)
	GetSubjectBySchema(schema *srclient.Schema) (string, error)
}
