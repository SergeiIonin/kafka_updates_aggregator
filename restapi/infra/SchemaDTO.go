package infra

import (
	"encoding/json"
	"kafka_updates_aggregator/restapi/domain"
	"log"
)

type SchemaDTO struct {
	Subject string     `json:"subject"`
	Type    string     `json:"type"`
	Name    string     `json:"name"`
	Fields  []FieldDTO `json:"fields"`
}

func (s SchemaDTO) ToJson() ([]byte, error) {
	schema := struct {
		Type   string     `json:"type"`
		Name   string     `json:"name"`
		Fields []FieldDTO `json:"fields"`
	}{
		Type:   s.Type,
		Name:   s.Name,
		Fields: s.Fields,
	}
	res, err := json.Marshal(schema)
	if err != nil {
		log.Printf("Error marshalling SchemaDTO: %v", err)
	}
	return res, err
}

type FieldDTO struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type SchemaRawDTO struct {
	domain.SchemaRawWithSubject
}
