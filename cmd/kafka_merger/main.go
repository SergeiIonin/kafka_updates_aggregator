package main

import (
	"context"
	"fmt"
	configs "kafka_updates_aggregator/configs"
	mergerConfig "kafka_updates_aggregator/configs/kafka_merger"
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
	basePath := fmt.Sprintf("%s/configs/kafka_merger", pwd)

	configTplPath = configs.GetPath(basePath, "templates/merger_config_template.yaml")
	valuesPath = configs.GetPath(basePath, "values.yaml")
	configPath = configs.GetPath(basePath, "merger_config.yaml")

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
	var kafkaMergerConfig mergerConfig.KafkaMergerConfig
	err := yaml.Unmarshal(configFile, &kafkaMergerConfig)
	if err != nil {
		log.Fatalf("Error parsing kafkaMergerConfig file: %v", err)
	}

	conf := kafkaMergerConfig.KafkaMerger
	kafkaMerger := kafkamerger.NewKafkaMerger(conf.Brokers, conf.SourceTopics, conf.GroupID, conf.MergedSourceTopic)
	ctx := context.Background()
	kafkaMerger.Run(ctx)
}
