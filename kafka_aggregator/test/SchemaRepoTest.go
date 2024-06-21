package test

import "github.com/riferrei/srclient"

type SchemaRepoTest struct {
	FieldToSchemas     map[string][]*srclient.Schema
	FieldToNamespaces  map[string][]string
	SubjectToNamespace map[string]string
	Subject            string
}

func NewSchemaRepoTest(subject string) *SchemaRepoTest {
	return &SchemaRepoTest{
		FieldToSchemas:     make(map[string][]*srclient.Schema),
		FieldToNamespaces:  make(map[string][]string),
		SubjectToNamespace: make(map[string]string),
		Subject:            subject,
	}
}

func (srt *SchemaRepoTest) AddSchemaToField(field string, schema *srclient.Schema) error {
	srt.FieldToSchemas[field] = append(srt.FieldToSchemas[field], schema)
	return nil
}

func (srt *SchemaRepoTest) AddNamespaceToField(field string, namespace string) error {
	srt.FieldToNamespaces[field] = append(srt.FieldToNamespaces[field], namespace)
	return nil
}

func (srt *SchemaRepoTest) AddSubjectToNamespace(subject string, namespace string) error {
	srt.SubjectToNamespace[subject] = namespace
	return nil
}

func (srt *SchemaRepoTest) GetSchemasByField(field string) ([]*srclient.Schema, error) {
	return srt.FieldToSchemas[field], nil
}

func (srt *SchemaRepoTest) GetNamespacesByField(field string) ([]string, error) {
	return srt.FieldToNamespaces[field], nil
}

func (srt *SchemaRepoTest) GetNamespaceBySubject(subject string) (string, error) {
	return srt.SubjectToNamespace[subject], nil
}

func (srt *SchemaRepoTest) GetSubjectBySchema(schema *srclient.Schema) (string, error) {
	return srt.Subject, nil
}
