package main

import (
	"context"
	"fmt"
	configs "kafka_updates_aggregator/configs"
	kafkamerger "kafka_updates_aggregator/kafka_merger"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

var (
	configTplPath string
	valuesPath    string
	configPath    string
	configFile    []byte
)

func init() {
	pwd, _ := os.Getwd()
	configTplPath = fmt.Sprintf("%s/configs/kafka_merger/templates/merger_config_template.yaml", pwd)
	valuesPath = fmt.Sprintf("%s/configs/kafka_merger/values.yaml", pwd)
	configPath = fmt.Sprintf("%s/configs/kafka_merger/merger_config.yaml", pwd)

	configReader := configs.NewConfigReader()

	err := configReader.ReadConfig(configTplPath, valuesPath, configPath)
	if err != nil {
		log.Fatalf("Error creating merger_config: %v", err)
	}
	configFile, err = os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Error reading kafka_merger config file: %v", err)
	}
}

func main() {
	var kafkaMergerConfig kafkamerger.KafkaMergerConfig
	err := yaml.Unmarshal(configFile, &kafkaMergerConfig)
	if err != nil {
		log.Fatalf("Error parsing kafkaMergerConfig file: %v", err)
	}
	log.Printf("KafkaMergerConfig: %+v", kafkaMergerConfig)

	conf := kafkaMergerConfig.KafkaMerger
	kafkaMerger := kafkamerger.NewKafkaMerger(conf.Brokers, conf.SourceTopics, conf.GroupID, conf.MergedSourceTopic)
	ctx := context.Background()
	kafkaMerger.Run(ctx)
}
