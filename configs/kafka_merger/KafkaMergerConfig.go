package configs

type KafkaMergerConfig struct {
	KafkaMerger struct {
		SourceTopics      []string `yaml:"source_topics"`
		Brokers           []string `yaml:"brokers"`
		GroupID 		  string   `yaml:"group_id"`
		MergedSourceTopic string   `yaml:"merged_source_topic"`
	} `yaml:"kafka_merger"`
}
