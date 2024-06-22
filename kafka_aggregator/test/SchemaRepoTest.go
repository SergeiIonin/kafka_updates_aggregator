package test

import "github.com/riferrei/srclient"

type SchemaRepoTest struct {
	FieldToSchemas map[string][]*srclient.Schema
	Subject        string
}

func NewSchemaRepoTest(subject string) *SchemaRepoTest {
	return &SchemaRepoTest{
		FieldToSchemas: make(map[string][]*srclient.Schema),
		Subject:        subject,
	}
}

func (srt *SchemaRepoTest) AddSchemaToField(field string, schema *srclient.Schema) error {
	srt.FieldToSchemas[field] = append(srt.FieldToSchemas[field], schema)
	return nil
}

func (srt *SchemaRepoTest) GetSchemasByField(field string) ([]*srclient.Schema, error) {
	return srt.FieldToSchemas[field], nil
}

func (srt *SchemaRepoTest) GetSubjectBySchema(schema *srclient.Schema) (string, error) {
	return srt.Subject, nil
}
