package application

import (
	"kafka_updates_aggregator/restapi/domain"
	"kafka_updates_aggregator/restapi/infra"

)

type SchemaAggregationService struct {
	schemaManager *domain.SchemaManager
}

func NewSchemaAggregationService(schemaManager *domain.SchemaManager) *SchemaAggregationService {
	return &SchemaAggregationService{
		schemaManager: schemaManager,
	}
}

func (srv *SchemaAggregationService) CreateAggregationSchema(schema infra.SchemaDTO) (int, error) {
	schemaJson, err := schema.ToJson()
	if err != nil {
		return -1, err
	}
	return srv.schemaManager.CreateSchema(schema.Subject, string(schemaJson))
}

func (srv *SchemaAggregationService) GetAggregationSchemas() ([]infra.SchemaRawDTO, error) {
	schemasWithSubject, err := srv.schemaManager.GetSchemas()
	schemasWithSubjectDTO := make([]infra.SchemaRawDTO, 0, len(schemasWithSubject))
	for _, schemaWithSubject := range schemasWithSubject {
		schemasWithSubjectDTO = append(schemasWithSubjectDTO, infra.SchemaRawDTO{SchemaRawWithSubject: schemaWithSubject})
	}
	return schemasWithSubjectDTO, err
}
