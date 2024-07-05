package test

import (
	"context"
	"fmt"
	"github.com/docker/docker/client"
	"github.com/segmentio/kafka-go"
	merger "kafka_updates_aggregator/kafka_merger"
	testutils "kafka_updates_aggregator/testutils"
	"log"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

var (
	kafkaBroker       = "localhost:9092"
	kafkaAddr         = kafka.TCP(kafkaBroker)
	topics            = []string{"test_1", "test_2", "test_3"}
	mergedSourceTopic = "test_merged"
	msgsPerTopic      = 10
	numTopics         = len(topics)
	kafka_client      *kafka.Client
	containerId       string
	dockerClient      *client.Client
)

func init() {
	var err error
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
}

// todo add test_containers support and ensure test_kafka_aggregator topics exist and have messages before the test_kafka_aggregator runs
func Test_KafkaMerger_Concurrent_test(t *testing.T) {
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

	testWriter := testutils.KafkaTestWriter{
		Writer: &kafka.Writer{
			Addr:     kafkaAddr,
			Balancer: &kafka.LeastBytes{},
		},
	}

	testReader := testutils.KafkaTestReader{
		kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaBroker},
			Topic:    mergedSourceTopic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
	}

	go writeTestMessagesWithInterleaving(&testWriter)
	go merger.Merge(context.Background())
	numMsgsTotal := msgsPerTopic * numTopics
	count := 0
	messages, err := testReader.ReadPlain(numMsgsTotal, &count)

	log.Printf("Merged source topic: %s", mergedSourceTopic)

	if err != nil {
		log.Printf("error reading messages %v", err)
	}

	timestamps := getTimestampsFromHeaders(messages)

	log.Printf("all timestamps: %v", timestamps)
	log.Printf("timestamps is sorted: %v", slices.IsSorted(timestamps))

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

func getTimestampsFromHeaders(messages []kafka.Message) []int64 {
	times := make([]int64, 0, len(messages))
	headersMap := make(map[string][]byte)
	for _, m := range messages {
		log.Printf("Header: %s", m.Headers)
		for _, h := range m.Headers {
			headersMap[h.Key] = h.Value
		}
		ts, _ := strconv.ParseInt(string(headersMap["time"]), 10, 64)
		times = append(times, ts)
	}
	return times
}

// In this case messages from different topics can interleave w/ each other in time
func writeTestMessagesWithInterleaving(writer *testutils.KafkaTestWriter) {
	log.Printf("Writing messages with interleaving")
	wg := &sync.WaitGroup{}
	ctx := context.Background()

	for _, topic := range topics {
		topicWriter := kafka.Writer{
			Addr:     kafkaAddr,
			Balancer: &kafka.LeastBytes{},
		}
		go func(topic string) {
			defer func() {
				wg.Done()
				if err := topicWriter.Close(); err != nil {
					log.Fatalf("could not close writer %v", err)
					return
				}
			}()
			wg.Add(1)
			msgs := writer.MakeMessagesForTopic(topic, msgsPerTopic)
			for _, msg := range msgs {
				log.Printf("Writing message %s", string(msg.Value))
				if err := topicWriter.WriteMessages(ctx, msg); err != nil {
					log.Fatalf("could not write messages %v", err)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}(topic)
	}
}
