package main

import (
	"context"
	"fmt"
	"kafka_updates_aggregator/configs"
	kafkaSchemasHandler "kafka_updates_aggregator/kafka_schemas_handler"
	schemasHandlerConfig "kafka_updates_aggregator/configs/kafka_schemas_handler"

	schemaswriter "kafka_updates_aggregator/kafka_schemas_handler/schemaswriter"
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
	configTplPath = fmt.Sprintf("%s/configs/kafka_schemas_handler/templates/schemas_handler_config_template.yaml", pwd)
	valuesPath = fmt.Sprintf("%s/configs/kafka_schemas_handler/values.yaml", pwd)
	configPath = fmt.Sprintf("%s/configs/kafka_schemas_handler/schemas_handler_config.yaml", pwd)

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
	var kafkaSchemasHandlerConfig schemasHandlerConfig.KafkaSchemasHandlerConfig
	err := yaml.Unmarshal(configFile, &kafkaSchemasHandlerConfig)
	if err != nil {
		log.Fatalf("Error parsing kafkaSchemasHandlerConfig file: %v", err)
	}
	log.Printf("kafkaSchemasHandlerConfig: %+v", kafkaSchemasHandlerConfig)

	conf := kafkaSchemasHandlerConfig.KafkaSchemasHandler

	redisAddr := conf.RedisAddress
	kafkaBrokers := conf.Brokers
	schemasWriter := schemaswriter.NewSchemasRedisWriter(redisAddr)
	KafkaSchemasHandler := kafkaSchemasHandler.NewKafkaSchemasHandler(kafkaBrokers[0], schemasWriter)
	ctx := context.Background()
	KafkaSchemasHandler.Run(ctx)
}
