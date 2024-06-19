package test

import (
	"context"
	"fmt"
	"github.com/docker/docker/client"
	"github.com/segmentio/kafka-go"
	merger "kafka_updates_aggregator/kafka_merger/infra/kafka/merger"
	testutils "kafka_updates_aggregator/testutils"
	"log"
	"slices"
	"testing"
	"time"
)

var (
	kafkaBroker       = "localhost:9092"
	kafkaAddr         = kafka.TCP(kafkaBroker)
	topics            = []string{"test_1", "test_2", "test_3"}
	mergedSourceTopic = "test_merged"
	msgsPerTopic      = 7
	numTopics         = len(topics)
	kafka_client      *kafka.Client
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

	kafka_client = &kafka.Client{
		Addr:      kafkaAddr,
		Transport: nil,
	}

	allTopics := append(topics, mergedSourceTopic)

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

	testWriter := testutils.KafkaTestWriter{
		Writer: &kafka.Writer{
			Addr:     kafkaAddr,
			Balancer: &kafka.LeastBytes{},
		},
	}

	kafkaMsgs := make([]kafka.Message, 0, numTopics*msgsPerTopic)
	for _, topic := range topics {
		kafkaMsgs = append(kafkaMsgs, testWriter.MakeMessagesForTopic(topic, msgsPerTopic)...)
	}
	if err := testWriter.Write(kafkaMsgs); err != nil {
		log.Fatalf("could not write messages %v", err)
	}
	err := testWriter.Close()
	if err != nil {
		log.Fatalf("could not close writer %v", err)
		return
	}
}

// todo add test_containers support and ensure test topics exist and have messages before the test runs
func TestKafkaMerger_test(t *testing.T) {
	cleanup := func() {
		testutils.CleanupAndGracefulShutdown(t, dockerClient, containerId)
	}

	//defer cleanup() // fixme it'd be great to rm containers in case t.Cleanup won't affect them
	t.Cleanup(cleanup)

	merger := merger.KafkaMerger{
		Brokers:           []string{kafkaBroker},
		Topics:            topics,
		GroupId:           fmt.Sprintf("new_%s", time.Now().String()),
		MergedSourceTopic: mergedSourceTopic,
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		select {
		case <-time.After(30 * time.Second):
			t.Logf("30 seconds elapsed")
			cancel()
			return
		}
	}()

	merger.Merge(ctx)

	log.Printf("Merged source topic: %s", mergedSourceTopic)

	testReader := testutils.KafkaTestReader{
		kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaBroker},
			Topic:    mergedSourceTopic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
	}

	numMsgsTotal := msgsPerTopic * numTopics
	count := 0
	messages, err := testReader.Read(numMsgsTotal, &count)

	if err != nil {
		log.Printf("error reading messages %v", err)
	}
	t.Logf("Messages: %d", count)
	if count != numMsgsTotal {
		t.Fatalf("Expected %d messages, got %d", numMsgsTotal, count)
	}

	offsetsPerTopicMap := testutils.GetOffsetsPerTopic(messages)
	offsetsPerTopic := make([]int64, msgsPerTopic)
	sorted := false
	for _, topic := range topics {
		offsetsPerTopic = offsetsPerTopicMap[topic]
		log.Printf("Topic: %s, Offsets: %v", topic, offsetsPerTopic)
		sorted = slices.IsSorted(offsetsPerTopic)
		if !sorted {
			t.Fatalf("Offsets for topic %s are not sorted", topic)
		}
	}

}
