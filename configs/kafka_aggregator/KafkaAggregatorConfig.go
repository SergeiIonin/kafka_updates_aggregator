package configs

type KafkaAggregatorConfig struct {
	KafkaAggregator struct {
		Broker            string   `yaml:"broker"`
		MergedSourceTopic string   `yaml:"merged_source_topic"`
		GroupID 		  string   `yaml:"group_id"`
		RedisAddress 	  string   `yaml:"redis_address"`
	} `yaml:"kafka_aggregator"`
}
