package test_kafka_schemas_handler

import (
	"kafka_updates_aggregator/domain"
	"log"
	"sync"
)

// regular map is used deliberately to have an option to range over it
type SchemasWriterTestImpl struct {
	Underlying map[string][]domain.Schema
	mut        sync.Mutex
}

func NewSchemasWriterTestImpl() *SchemasWriterTestImpl {
	return &SchemasWriterTestImpl{
		Underlying: make(map[string][]domain.Schema),
		mut:        sync.Mutex{},
	}
}

func (sw *SchemasWriterTestImpl) SaveSchema(schema domain.Schema) (string, error) {
	log.Printf("saving schema %v", schema)
	for _, field := range schema.Fields {
		schemas, ok := sw.Underlying[field]
		contains := false
		if ok {
			for _, s := range schemas {
				subject := s.Subject
				id := s.ID
				if (subject == schema.Subject) && (id == schema.ID) {
					contains = true
					break
				}
			}
		} else {
			sw.mut.Lock()
			sw.Underlying[field] = []domain.Schema{schema}
			sw.mut.Unlock()
		}
		if !contains {
			sw.mut.Lock()
			sw.Underlying[field] = append(schemas, schema)
			sw.mut.Unlock()
		}
	}
	log.Printf("underlying map: %v", sw.Underlying)
	return schema.Subject, nil
}

func (sw *SchemasWriterTestImpl) DeleteSchema(subject string, version int) (string, error) {
	for field, schemas := range sw.Underlying {
		i := -1
		for j, s := range schemas {
			if s.Subject == subject && s.ID == version {
				i = j
			}
		}
		if i != -1 {
			schemas = append(schemas[:i], schemas[i+1:]...)
			sw.mut.Lock()
			sw.Underlying[field] = schemas
			sw.mut.Unlock()
		}
	}
	return subject, nil
}
