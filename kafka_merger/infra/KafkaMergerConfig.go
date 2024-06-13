package infra

type KafkaMergerConfig struct {
	KafkaMerger struct {
		Topics   []string `yaml:"topics"`
		Hostname string   `yaml:"hostname"`
	} `yaml:"kafka_merger"`
}
