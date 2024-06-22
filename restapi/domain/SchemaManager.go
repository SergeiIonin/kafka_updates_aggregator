package domain

type SchemaManager struct {
	SchemaClient
}

func NewSchemaManager(schemaClient SchemaClient) *SchemaManager {
	return &SchemaManager{schemaClient}
}

type SchemaClient interface {
	CreateSchema(subject string, schemaRaw string) (int, error)
	GetSchemas() ([]SchemaRawWithSubject, error)
}
