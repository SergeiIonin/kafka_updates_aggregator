package test

import (
	"context"
	"encoding/json"
	"github.com/docker/docker/client"
	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/kafka_aggregator"
	"kafka_updates_aggregator/testutils"
	"log"
	"testing"
)

var (
	kafkaBroker          = "localhost:9092"
	kafkaAddr            = kafka.TCP(kafkaBroker)
	mergedSourceTopic    = "test_merged"
	aggregateTopic       = "user_balance_updates"
	kafka_client         *kafka.Client
	containerId          string
	err                  error
	dockerClient         *client.Client
	schemaRegistryClient srclient.ISchemaRegistryClient
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

	kafka_client = &kafka.Client{
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

	if _, err = kafka_client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		kafkaAddr,
		topicConfigs,
		false,
	},
	); err != nil {
		log.Fatalf("could not create topics %v", err)
	}

	schemaRegistryClient = srclient.CreateMockSchemaRegistryClient("http://localhost:8081")
}

func TestKafkaAggregator_test(t *testing.T) {
	/*cleanup := func() {
		testutils.CleanupAndGracefulShutdown(t, dockerClient, containerId)
	}

	//defer cleanup() // fixme it'd be great to rm containers in case t.Cleanup won't affect them
	t.Cleanup(cleanup)*/

	cache := NewCacheTest()
	schemaService := NewSchemaServiceTest(aggregateTopic)
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    mergedSourceTopic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	kafkaWriter := &kafka.Writer{
		Addr:     kafkaAddr,
		Balancer: &kafka.LeastBytes{},
	}
	aggregator := kafka_aggregator.KafkaAggregator{
		kafkaReader,
		kafkaWriter,
		schemaService,
		cache,
	}

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

	schema, err := schemaRegistryClient.CreateSchema(aggregateTopic, schemaRaw, srclient.Json, srclient.Reference{
		Name:    aggregateTopic,
		Subject: aggregateTopic,
		Version: 0,
	})
	refs := schema.References()
	refs = append(refs, srclient.Reference{
		Name:    aggregateTopic,
		Subject: aggregateTopic,
		Version: 0,
	})

	userId := "bob"
	schemaService.SaveSchema(schema, "users")

	cache.CreateNamespace("users")
	cache.Create("users", userId, "balance", 1000)
	cache.Create("users", userId, "deposit", 500)
	cache.Create("users", userId, "withdrawal", 200)

	payload := []byte(`{"balance": 1200, "deposit": 700, "isAuthenticated": true, "country": "Cordovia"}`)

	message := kafka.Message{
		Topic: aggregateTopic,
		Key:   []byte(userId),
		Value: payload,
	}
	aggregator.WriteAggregate(userId, message)

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
		t.Fatalf("could not read messages %v", err)
	}

	value := make(map[string]interface{})
	err = json.Unmarshal(messages[0].Value, &value)
	if err != nil {
		t.Fatalf("could not unmarshal message %v", err)
	}
	t.Logf("value: %v", value)
	// fixme it's better to avoid float64 comparison
	if value["balance"] != float64(1200) && value["deposit"] != float64(700) && value["withdrawal"] != float64(200) {
		t.Fatalf("unexpected values %v", value)
	}

}
