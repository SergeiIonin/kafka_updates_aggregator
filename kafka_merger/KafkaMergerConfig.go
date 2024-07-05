package kafka_merger

type KafkaMergerConfig struct {
	KafkaMerger struct {
		SourceTopics      []string `yaml:"source_topics"`
		Hostname          string   `yaml:"hostname"`
		MergedSourceTopic string   `yaml:"merged_source_topic"`
	} `yaml:"kafka_merger"`
}
