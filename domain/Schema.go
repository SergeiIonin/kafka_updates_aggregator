package domain

import (
	"encoding/json"
	"fmt"
)

const FieldPrefix string = "field."
const SchemaPrefix string = "schema."

type Schema struct {
	subject string
	version int
	id      int
	fields  []Field
	schema  string // a raw json schema
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func CreateSchema(subject string, version int, id int, fields []Field, schema string) *Schema {
	return &Schema{subject, version, id, fields, schema}
}

// Key is the subject:version pair which is unique for the schema
func (s *Schema) Key() string {
	return fmt.Sprintf("%s.%d", s.subject, s.version)
}

func (s *Schema) Subject() string {
	return s.subject
}

func (s *Schema) Version() int {
	return s.version
}

func (s *Schema) ID() int {
	return s.id
}

func (s *Schema) Fields() []Field {
	return s.fields
}

func (s *Schema) Schema() string {
	return s.schema
}

func (s *Schema) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Subject string  `json:"subject"`
		Version int     `json:"version"`
		ID      int     `json:"id"`
		Fields  []Field `json:"fields"`
		Schema  string  `json:"schema"`
	}{
		Subject: s.subject,
		Version: s.version,
		ID:      s.id,
		Fields:  s.fields,
		Schema:  s.schema,
	})
}

func (s *Schema) UnmarshalJSON(data []byte) error {
	type Alias Schema
	aux := &struct {
		Subject string  `json:"subject"`
		Version int     `json:"version"`
		ID      int     `json:"id"`
		Fields  []Field `json:"fields"`
		Schema  string  `json:"schema"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	s.subject = aux.Subject
	s.version = aux.Version
	s.id = aux.ID
	s.fields = aux.Fields
	s.schema = aux.Schema
	return nil
}
