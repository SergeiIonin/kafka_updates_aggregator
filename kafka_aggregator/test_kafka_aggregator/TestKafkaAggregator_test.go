package test_kafka_aggregator

import (
	"context"
	"encoding/json"
	"github.com/docker/docker/client"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"kafka_updates_aggregator/domain"
	"kafka_updates_aggregator/kafka_aggregator"
	"kafka_updates_aggregator/testutils"
	"log"
	"testing"
)

var (
	kafkaBroker       = "localhost:9092"
	kafkaAddr         = kafka.TCP(kafkaBroker)
	mergedSourceTopic = "test_merged"
	aggregateTopic    = "user_balance_updates"
	kafkaClient       *kafka.Client
	containerId       string
	err               error
	dockerClient      *client.Client
)

func init() {
	dockerClient, err = client.NewClientWithOpts(client.WithVersion("1.45"))
	if err != nil {
		log.Printf("error creating docker client: %s", err.Error())
		panic(err)
	}
	containerId, err = testutils.CreateKafkaWithKRaftContainer(dockerClient)
	if err != nil {
		log.Fatalf("could not create container %v", err)
	}

	log.Printf("Container ID: %s", containerId)

	kafkaClient = &kafka.Client{
		Addr:      kafkaAddr,
		Transport: nil,
	}

	allTopics := []string{aggregateTopic, mergedSourceTopic}

	topicConfigs := make([]kafka.TopicConfig, len(allTopics))
	for i, topic := range allTopics {
		topicConfigs[i] = kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}

	if _, err = kafkaClient.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		kafkaAddr,
		topicConfigs,
		false,
	},
	); err != nil {
		log.Fatalf("could not create topics %v", err)
	}

}

func TestKafkaAggregator_test(t *testing.T) {
	cleanup := func() {
		testutils.CleanupAndGracefulShutdown(t, dockerClient, containerId)
	}
	//defer cleanup() // fixme it'd be great to rm containers in case t.Cleanup won't affect them
	t.Cleanup(cleanup)

	cache := NewFieldsCacheTest()
	fieldToSchemasMap := make(map[string][]domain.Schema)
	schemasReader := NewSchemasReaderTestImpl(fieldToSchemasMap)
	schemasWriter := NewSchemasWriterTest(fieldToSchemasMap)

	aggregator := kafka_aggregator.NewKafkaAggregator(kafkaBroker, mergedSourceTopic, schemasReader, cache)

	schemaRaw := `{
		"type": "record",
		"name": "user_balance_updates",
		"fields": [
			{"name": "user_id", "type": "string"},
			{"name": "balance", "type": "int"},
			{"name": "deposit", "type": "int"},
			{"name": "withdrawal", "type": "int"}
		]
	}`

	schema := domain.CreateSchema("user_balance_updates", 1,
		1,
		[]string{"user_id", "balance", "deposit", "withdrawal"},
		schemaRaw)

	userId := "bob"
	schemasWriter.AddSchemaToField("user_id", *schema)
	schemasWriter.AddSchemaToField("balance", *schema)
	schemasWriter.AddSchemaToField("deposit", *schema)
	schemasWriter.AddSchemaToField("withdrawal", *schema)

	cache.Add(userId, "user_id", "bob")
	cache.Add(userId, "balance", 1000)
	cache.Add(userId, "deposit", 500)
	cache.Add(userId, "withdrawal", 200)

	payload := []byte(`{"balance": 1200, "deposit": 700, "isAuthenticated": true, "country": "Cordovia"}`)

	message := kafka.Message{
		Topic: aggregateTopic,
		Key:   []byte(userId),
		Value: payload,
	}
	err = aggregator.WriteAggregate(context.Background(), userId, message)
	assert.NoError(t, err)

	testReader := testutils.KafkaTestReader{
		kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaBroker},
			Topic:    aggregateTopic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
	}

	count := 0
	messages, err := testReader.Read(1, &count)
	if err != nil {
		t.Fatalf("could not read messages: %v", err)
	}

	value := make(map[string]interface{})
	err = json.Unmarshal(messages[0].Value, &value)
	if err != nil {
		t.Fatalf("could not unmarshal message %v", err)
	}
	t.Logf("value: %v", value)

	convertToInt := func(i any) int64 {
		switch i.(type) {
		case int:
			return int64(i.(int))
		case float64:
			return int64(i.(float64))
		}
		t.Fatalf("unexpected type %T", i)
		return -1
	}

	if convertToInt(value["balance"]) != 1200 && convertToInt(value["deposit"]) != 700 &&
		convertToInt(value["withdrawal"]) != 200 {
		t.Fatalf("unexpected values %v", value)
	}

}
