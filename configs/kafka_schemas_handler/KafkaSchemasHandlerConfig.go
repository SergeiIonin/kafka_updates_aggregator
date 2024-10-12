package configs

type KafkaSchemasHandlerConfig struct {
	KafkaSchemasHandler struct {
		RedisAddress string   `yaml:"redis_address"`
		Brokers      []string `yaml:"brokers"`
		GroupID      string   `yaml:"group_id"`
	} `yaml:"kafka_schemas_handler"`
}
