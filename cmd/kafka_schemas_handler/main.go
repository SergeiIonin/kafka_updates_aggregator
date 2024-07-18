package main

import (
	"context"
	kafkaschemashandler "kafka_updates_aggregator/kafka_schemas_handler"
	schemaswriter "kafka_updates_aggregator/kafka_schemas_handler/schemaswriter"
)

func main() {
	kafkaBrokers := []string{"localhost:9092"}
	redisAddr := "localhost:6379"
	schemasWriter := schemaswriter.NewSchemasRedisWriter(redisAddr)
	KafkaSchemasHandler := kafkaschemashandler.NewKafkaSchemasHandler(kafkaBrokers[0], schemasWriter)
	ctx := context.Background()
	KafkaSchemasHandler.Run(ctx)
}
