package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"kafka_updates_aggregator/kafka_merger/infra"
	"log"
	"os"
)

func main() {
	configReader := infra.NewConfigReader()
	pwd, _ := os.Getwd()
	configTplPath := fmt.Sprintf("%s/kafka_merger/config/templates/merger_config_template.yaml", pwd)
	valuesPath := fmt.Sprintf("%s/kafka_merger/config/values.yaml", pwd)
	configPath := fmt.Sprintf("%s/kafka_merger/config/merger_config.yaml", pwd)

	err := configReader.ReadConfig(configTplPath, valuesPath, configPath)
	if err != nil {
		log.Fatalf("Error creating merger_config: %v", err)
	}
	configFile, err := os.ReadFile(configPath)

	var kafkaMergerConfig infra.KafkaMergerConfig
	err = yaml.Unmarshal(configFile, &kafkaMergerConfig)
	if err != nil {
		log.Fatalf("Error parsing kafkaMergerConfig file: %v", err)
	}

}
