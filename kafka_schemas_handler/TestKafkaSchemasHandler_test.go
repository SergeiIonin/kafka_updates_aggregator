package kafka_schemas_handler

import (
	"context"
	"fmt"
	"kafka_updates_aggregator/test"
	testkafkaschemashandler "kafka_updates_aggregator/test/kafka_schemas_handler"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/client"
	"github.com/segmentio/kafka-go"
)

var (
	kafkaBroker  = "localhost:9092"
	kafkaAddr    = kafka.TCP(kafkaBroker)
	schemasTopic = "_schemas"
	kafkaClient  *kafka.Client
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
	containerId, err = test.CreateKafkaWithKRaftContainer(dockerClient)
	if err != nil {
		log.Fatalf("could not create container %v", err)
	}

	log.Printf("Container ID: %s", containerId)

	kafkaClient = &kafka.Client{
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

	if _, err = kafkaClient.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
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
		test.CleanupAndGracefulShutdown(t, dockerClient, containerId)
	}
	//defer cleanup() // fixme it'd be great to rm containers in case t.Cleanup won't affect them
	t.Cleanup(cleanup)

	schemasWriter := testkafkaschemashandler.NewSchemasWriterTestImpl()

	t.Run("create schema and delete schema", func(t *testing.T) {
		wg := sync.WaitGroup{}
		subject := "foo"
		version := 1
		schemaID := fmt.Sprintf("%s-%d", subject, version)
		kafkaSchemasHandler := NewKafkaSchemasHandler(kafkaBroker, schemasWriter)
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
				if len(schemasWriter.Underlying) != 3 {
					time.Sleep(50 * time.Millisecond) // fixme otherwise we can get an error on concurrent reads and writes to map, we can write to the channel in the schema DAO itself in tests
					continue
				}
				_, firstNameOk := schemasWriter.Underlying["firstName"]
				_, lastNameOk := schemasWriter.Underlying["lastName"]
				_, ageOk := schemasWriter.Underlying["age"]
				if !firstNameOk || !lastNameOk || !ageOk {
					t.Fatalf("Schema %s is not added", schemaID)
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
				if schemas, _ := schemasWriter.Underlying["firstName"]; len(schemas) != 0 {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				scFirstName, _ := schemasWriter.Underlying["firstName"]
				scLastName, _ := schemasWriter.Underlying["lastName"]
				scAge, _ := schemasWriter.Underlying["age"]
				if len(scFirstName) != 0 || len(scLastName) != 0 || len(scAge) != 0 {
					t.Fatalf("Schema %s is not deleted", schemaID)
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
