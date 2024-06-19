package test

import kafkaAggregator "kafka_updates_aggregator/kafka_aggregator"

type SchemaServiceTest struct {
	FieldToSchemas    map[string][]kafkaAggregator.Schema
	FieldToNamespaces map[string][]string
}

func NewSchemaServiceTest() *SchemaServiceTest {
	return &SchemaServiceTest{
		FieldToSchemas:    map[string][]kafkaAggregator.Schema{},
		FieldToNamespaces: map[string][]string{},
	}
}

func (sst *SchemaServiceTest) CreateSchema(schema kafkaAggregator.Schema, namespace string) error {
	fields := schema.Fields
	for _, field := range fields {
		sst.FieldToSchemas[field] = append(sst.FieldToSchemas[field], schema)
		sst.FieldToNamespaces[field] = append(sst.FieldToNamespaces[field], namespace)
	}
	return nil
}

func (sst *SchemaServiceTest) GetSchemasForField(field string) ([]kafkaAggregator.Schema, error) {
	return sst.FieldToSchemas[field], nil
}
func (sst *SchemaServiceTest) GetNamespacesForField(field string) ([]string, error) {
	return sst.FieldToNamespaces[field], nil
}
