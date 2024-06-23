package test_kafka_schemas_handler

import (
	"context"
	"fmt"
	"github.com/docker/docker/client"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/kafka_schemas_handler/handler"
	"kafka_updates_aggregator/testutils"
	"log"
	"sync"
	"testing"
	"time"
)

var (
	kafkaBroker  = "localhost:9092"
	kafkaAddr    = kafka.TCP(kafkaBroker)
	schemasTopic = "_schemas"
	kafka_client *kafka.Client
	kafkaWriter  *kafka.Writer
	containerId  string
	err          error
	dockerClient *client.Client
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

	allTopics := []string{schemasTopic}

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

	kafkaWriter = &kafka.Writer{
		Addr:     kafkaAddr,
		Balancer: &kafka.LeastBytes{},
	}
}

func TestKafkaAggregator_test(t *testing.T) {
	cleanup := func() {
		testutils.CleanupAndGracefulShutdown(t, dockerClient, containerId)
	}
	//defer cleanup() // fixme it'd be great to rm containers in case t.Cleanup won't affect them
	t.Cleanup(cleanup)

	schemaDAO := NewSchemasDAOTestImpl()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    "_schemas",
		GroupID:  "schemas_handler",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	t.Run("create schema and delete schema", func(t *testing.T) {
		wg := sync.WaitGroup{}
		subject := "foo"
		version := 1
		schemaID := fmt.Sprintf("%s-%d", subject, version)
		kafkaSchemasHandler := handler.NewKafkaSchemasHandler(reader, schemaDAO)
		ctx, cancel := context.WithCancel(context.Background())

		go kafkaSchemasHandler.Run(ctx)

		go func() {
			time.Sleep(25 * time.Second)
			cancel()
		}()

		// check schema creation
		createSchemaMsg := kafka.Message{
			Topic:     schemasTopic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("{\"keytype\":\"SCHEMA\",\"subject\":\"%s\",\"version\":%d,\"magic\":1}", subject, version)),
			Value:     []byte(fmt.Sprintf("{\"subject\":\"%s\",\"version\":%d,\"id\":1,\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"users\\\",\\\"fields\\\":[{\\\"name\\\":\\\"firstName\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"lastName\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"age\\\",\\\"type\\\":\\\"int\\\"}]}\",\"deleted\":false}", subject, version)),
		}

		createSchema := func() {
			err := kafkaWriter.WriteMessages(ctx, createSchemaMsg)
			if err != nil {
				t.Fatalf("Error writing createSchemaMsg to _schemas: %v", err)
			}
		}

		schemaWrittenChan := make(chan bool)
		ensureSchemaWritten := func() {
			for {
				if _, ok := schemaDAO.Underlying[schemaID]; !ok {
					time.Sleep(50 * time.Millisecond) // fixme otherwise we can get an error on concurrent reads and writes to map, we can write to the channel in the schema DAO itself in tests
					continue
				}
				t.Logf("Schema %s is added", schemaID)
				schemaWrittenChan <- true
				break
			}
		}

		wg.Add(1)
		createSchema()
		go ensureSchemaWritten()
		select {
		case <-schemaWrittenChan:
			wg.Done()
			close(schemaWrittenChan)
			break
		case <-ctx.Done():
			t.Fatalf("timeout waiting for schema to be written")
		}
		wg.Wait()

		// check schema deletion
		deleteSubjectMsg := kafka.Message{
			Topic:     schemasTopic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("{\"keytype\":\"DELETE_SUBJECT\",\"subject\":\"%s\",\"magic\":0}", subject)),
			Value:     []byte(fmt.Sprintf("{\"subject\":\"%s\",\"version\":%d}", subject, version)),
		}

		deleteSchema := func() {
			err := kafkaWriter.WriteMessages(ctx, deleteSubjectMsg)
			if err != nil {
				t.Fatalf("Error writing deleteSubjectMsg to _schemas: %v", err)
			}
		}

		schemaDeletedChan := make(chan bool)
		ensureSchemaDeleted := func() {
			for {
				if _, ok := schemaDAO.Underlying[schemaID]; ok {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				t.Logf("Schema %s is deleted", schemaID)
				schemaDeletedChan <- true
				break
			}
		}

		wg.Add(1)
		deleteSchema()
		go ensureSchemaDeleted()
		select {
		case <-schemaDeletedChan:
			wg.Done()
			close(schemaDeletedChan)
			break
		case <-ctx.Done():
			t.Fatalf("timeout waiting for schema to be deleted")
		}
		wg.Wait()
	})
}
