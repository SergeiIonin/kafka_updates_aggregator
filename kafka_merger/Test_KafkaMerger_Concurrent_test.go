package kafka_merger

import (
	"context"
	"fmt"
	"github.com/docker/docker/client"
	"github.com/segmentio/kafka-go"
	test "kafka_updates_aggregator/test"
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
		log.Fatalf("error creating docker client: %s", err.Error())
	}
	containerId, err = test.CreateKafkaWithKRaftContainer(dockerClient)
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
		Addr: kafkaAddr,
		Topics: topicConfigs,
		ValidateOnly: false,
	},
	); err != nil {
		log.Fatalf("could not create topics %v", err)
	}
}

// todo add test_containers support and ensure test_kafka_aggregator topics exist and have messages before the test_kafka_aggregator runs
// should pass in 30s
func Test_KafkaMerger_Concurrent_test(t *testing.T) {
	
	cleanup := func() {
		test.CleanupAndGracefulShutdown(t, dockerClient, containerId)
	}

	t.Cleanup(cleanup)

	merger := KafkaMerger{
		Brokers:           []string{kafkaBroker},
		Topics:            topics,
		GroupId:           fmt.Sprintf("new_%s", time.Now().String()),
		MergedSourceTopic: mergedSourceTopic,
	}

	testWriter := test.KafkaTestWriter{
		Writer: &kafka.Writer{
			Addr:     kafkaAddr,
			Balancer: &kafka.LeastBytes{},
		},
	}

	testReader := test.KafkaTestReader{
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaBroker},
			Topic:    mergedSourceTopic,
			MinBytes: 1e3, // 1KB
			MaxBytes: 10e6, // 10MB
			ReadBackoffMin: 5 * time.Millisecond,
			ReadBackoffMax: 10 * time.Millisecond,
		}),
	}

	go merger.Run(context.Background())

	go writeTestMessagesWithInterleaving(&testWriter)
	
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

	offsetsPerTopicMap := test.GetOffsetsPerTopic(messages)
	sorted := false
	for _, topic := range topics {
		offsetsPerTopic := offsetsPerTopicMap[topic]
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
func writeTestMessagesWithInterleaving(writer *test.KafkaTestWriter) {
	log.Printf("Writing messages with interleaving")
	wg := &sync.WaitGroup{}
	ctx := context.Background()

	wg.Add(len(topics))

	for _, topic := range topics {
		topicWriter := kafka.Writer {
			Addr:     kafkaAddr,
			WriteBackoffMin: 1 * time.Millisecond,
			WriteBackoffMax: 5 * time.Millisecond,
			BatchTimeout:   1 * time.Millisecond,
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
			msgs := writer.MakeMessagesForTopic(topic, msgsPerTopic)
			for _, msg := range msgs {
				log.Printf("Writing message %s at %d", string(msg.Value), time.Now().UnixMilli())
				if err := topicWriter.WriteMessages(ctx, msg); err != nil {
					log.Fatalf("could not write messages %v", err)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(topic)
	}
	wg.Wait()
	log.Printf("Finished writing test messages")
}
