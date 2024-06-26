package domain

import (
	"encoding/json"
	"fmt"
)

// fixme fields should be unexpported and we can only access them
type Schema struct {
	subject string
	version int
	id      int
	fields  []string
	schema  string // a raw json schema
}

func CreateSchema(subject string, version int, id int, fields []string, schema string) *Schema {
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

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) Schema() string {
	return s.schema
}

func (s *Schema) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Subject string   `json:"subject"`
		Version int      `json:"version"`
		ID      int      `json:"id"`
		Fields  []string `json:"fields"`
		Schema  string   `json:"schema"`
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
		Subject string   `json:"subject"`
		Version int      `json:"version"`
		ID      int      `json:"id"`
		Fields  []string `json:"fields"`
		Schema  string   `json:"schema"`
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
