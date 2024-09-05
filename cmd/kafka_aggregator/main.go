package main

import (
	"context"
	"fmt"
	configs "kafka_updates_aggregator/configs"
	aggregatorConfig "kafka_updates_aggregator/configs/kafka_aggregator"
	kafkaaggregator "kafka_updates_aggregator/kafka_aggregator"
	schemasreader "kafka_updates_aggregator/kafka_aggregator/schemasreader"
	fieldscache "kafka_updates_aggregator/kafka_aggregator/fieldscache"
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
	basePath := fmt.Sprintf("%s/configs/kafka_aggregator", pwd)
	configTplPath = configs.GetPath(basePath, "templates/aggregator_config_template.yaml")
	valuesPath = configs.GetPath(basePath, "values.yaml")
	configPath = configs.GetPath(basePath, "aggregator_config.yaml")

	configReader := configs.NewConfigReader()

	err := configReader.ReadConfig(configTplPath, valuesPath, configPath)
	if err != nil {
		log.Fatalf("Error creating aggregator_config: %v", err)
	}
	
	configFile, err = os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Error reading kafka_aggregator config file: %v", err)
	}
}

func main() {
	var kafkaAggregatorConfig aggregatorConfig.KafkaAggregatorConfig
	err := yaml.Unmarshal(configFile, &kafkaAggregatorConfig)
	if err != nil {
		log.Fatalf("Error parsing kafkaAggregatorConfig file: %v", err)
	}

	conf := kafkaAggregatorConfig.KafkaAggregator
	schemasReader := schemasreader.NewSchemasRedisReader(conf.RedisAddress)
	fieldsCache := fieldscache.NewFieldsRedisCache(conf.RedisAddress)
	kafkaAggregator := kafkaaggregator.NewKafkaAggregator(conf.Broker, conf.MergedSourceTopic, schemasReader, fieldsCache)
	ctx := context.Background()
	kafkaAggregator.Run(ctx)
}