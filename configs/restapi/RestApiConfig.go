package configs

type RestApiConfig struct {
	RestApi struct {
		Host			  string   `yaml:"host"`
		Port			  int   	`yaml:"port"`
		SchemaRegistryAddress string `yaml:"schema_registry_address"`
	} `yaml:"rest_api"`
}
