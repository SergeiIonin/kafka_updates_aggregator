package test

import (
	"encoding/json"
	"fmt"
	"github.com/riferrei/srclient"
)

type SchemaServiceTest struct {
	FieldToSchemas     map[string][]*srclient.Schema
	FieldToNamespaces  map[string][]string
	SubjectToNamespace map[string]string
	Subject            string
}

func NewSchemaServiceTest(subject string) *SchemaServiceTest {
	return &SchemaServiceTest{
		FieldToSchemas:     map[string][]*srclient.Schema{},
		FieldToNamespaces:  map[string][]string{},
		SubjectToNamespace: map[string]string{},
		Subject:            subject,
	}
}

func (sst *SchemaServiceTest) SaveSchema(schema *srclient.Schema, namespace string) error {
	fields, _ := sst.GetSchemaFields(schema)
	subject, err := sst.GetSchemaSubject(schema)
	if err != nil {
		return fmt.Errorf("could not get subject for schema %v", err)
	}
	for _, field := range fields {
		sst.FieldToSchemas[field] = append(sst.FieldToSchemas[field], schema)
		sst.FieldToNamespaces[field] = append(sst.FieldToNamespaces[field], namespace)
		sst.SubjectToNamespace[subject] = namespace
	}
	return nil
}

// todo support avro and protobuf as well
func (sst *SchemaServiceTest) GetSchemaSubject(schema *srclient.Schema) (string, error) {
	schemaType := schema.SchemaType()
	if *schemaType != srclient.Json {
		return "", fmt.Errorf("unsupported schema type %v", *schemaType)
	}
	// if schema has multiple References then they can be different in only Version (Subject and Name should be the same)
	// tests only
	subject := sst.Subject
	//subject := schema.References()[0].Subject
	return subject, nil
}

func (sst *SchemaServiceTest) GetSchemasForField(field string) ([]*srclient.Schema, error) {
	return sst.FieldToSchemas[field], nil
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

func (sst *SchemaServiceTest) GetSchemaFields(schema *srclient.Schema) ([]string, error) {
	schemaType := schema.SchemaType()
	if *schemaType != srclient.Json {
		return nil, fmt.Errorf("unsupported schema type %v", *schemaType)
	}
	//res := make(map[string]any)
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

func (sst *SchemaServiceTest) GetNamespacesForField(field string) ([]string, error) {
	return sst.FieldToNamespaces[field], nil
}

func (sst *SchemaServiceTest) GetNamespaceBySubject(subject string) (string, error) {
	return sst.SubjectToNamespace[subject], nil
}

/*func (sst *SchemaServiceTest) GetNamespacesForField(field string) ([]string, error) {
	schemasForField, err := sst.GetSchemasForField(field)
	if err != nil {
		return nil, fmt.Errorf("could not get schemas for field %s, %v", field, err)
	}
	namespacesForField := make([]string, 0, len(schemasForField))
	for _, schema := range schemasForField {
		subject, err := sst.GetSchemaSubject(schema)
		if err != nil {
			return nil, fmt.Errorf("could not get subject for schema %v", err)
		}
		namespace, err := sst.GetNamespaceBySubject(subject)
		if err != nil {
			return nil, fmt.Errorf("could not get namespace for subject %v", err)
		}
		namespacesForField = append(namespacesForField, namespace)
	}
	return namespacesForField, nil
}*/
