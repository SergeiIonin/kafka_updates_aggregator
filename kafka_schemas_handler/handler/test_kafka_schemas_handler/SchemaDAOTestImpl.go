package test_kafka_schemas_handler

import (
	"fmt"
	"sync"
)

type SchemasDAOTestImpl struct {
	Underlying map[string]string
	mutex      *sync.Mutex
}

func NewSchemasDAOTestImpl() *SchemasDAOTestImpl {
	return &SchemasDAOTestImpl{
		Underlying: make(map[string]string),
		mutex:      &sync.Mutex{},
	}
}

func getSubjectID(subject string, version int) string {
	return fmt.Sprintf("%s-%d", subject, version)
}

func (dao *SchemasDAOTestImpl) SaveSchema(subject string, version int, id int, schema string) (string, error) {
	schemaID := getSubjectID(subject, version)
	dao.mutex.Lock()
	dao.Underlying[schemaID] = schema
	dao.mutex.Unlock()
	return schemaID, nil
}

func (dao *SchemasDAOTestImpl) DeleteSchema(subject string, version int) (string, error) {
	schemaID := getSubjectID(subject, version)
	dao.mutex.Lock()
	delete(dao.Underlying, schemaID)
	dao.mutex.Unlock()
	return schemaID, nil
}
