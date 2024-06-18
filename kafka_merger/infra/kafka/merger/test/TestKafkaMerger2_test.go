package test

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/segmentio/kafka-go"
	merger2 "kafka_updates_aggregator/kafka_merger/infra/kafka/merger"
	"kafka_updates_aggregator/kafka_merger/infra/kafka/merger/test/testutils"
	"log"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	kafkaBroker       = "localhost:9092"
	kafkaAddr         = kafka.TCP(kafkaBroker)
	topics            = []string{"test_1", "test_2", "test_3"}
	mergedSourceTopic = "test_merged"
	msgsPerTopic      = 15
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
	containerId, err = CreateKafkaWithKRaftContainer(dockerClient)
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
func TestKafkaMerger2_test(t *testing.T) {
	//defer CleanupAndGracefulShutdown(t, dockerClient, containerId)

	merger := merger2.KafkaMerger2{
		Brokers:           []string{kafkaBroker},
		Topics:            topics,
		GroupId:           fmt.Sprintf("new_%s", time.Now().String()),
		MergedSourceTopic: mergedSourceTopic,
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	count := 0

	go func() {
		select {
		case <-time.After(25 * time.Second):
			t.Logf("25 seconds elapsed")
			cancel()
			return
		}
	}()

	wg.Add(len(topics))
	merger.Merge(ctx, wg, &sync.Mutex{}, &count)
	wg.Wait()

	t.Logf("Messages: %d", count)
	numMsgsTotal := msgsPerTopic * numTopics
	if count != numMsgsTotal {
		t.Fatalf("Expected 45 messages, got %d", count)
	}

	testReader := testutils.KafkaTestReader{
		kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaBroker},
			Topic:    mergedSourceTopic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
	}

	messages := make([]kafka.Message, numMsgsTotal)

	if err = testReader.Read(messages); err != nil {
		log.Printf("could not read messages %v", err)
	}

	offsetsPerTopicMap := GetOffsetsPerTopic(messages)
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

// This is not an actual offsets from the merged data topic, but the offsets from the original topics, which is
// presented in each message's value as a suffix, e.g. value-test_2-7, where test_2 is the original topic and 7 is the offset
func GetOffsetsPerTopic(messages []kafka.Message) map[string][]int64 {
	offsetsPerTopic := make(map[string][]int64)
	getOffset := func(value string) int64 {
		elems := strings.Split(value, "-")
		offset, _ := strconv.ParseInt(elems[len(elems)-1], 10, 64)
		return offset
	}
	var header string
	var offset int64
	for _, m := range messages {
		header = string(m.Headers[0].Value)
		if _, ok := offsetsPerTopic[header]; !ok {
			offsetsPerTopic[header] = make([]int64, 0)
		}
		offset = getOffset(string(m.Value))
		offsetsPerTopic[header] = append(offsetsPerTopic[header], offset)
	}
	log.Printf("Offsets per topic: %v", offsetsPerTopic)
	return offsetsPerTopic
}

func CleanupAndGracefulShutdown(t *testing.T, dockerClient *client.Client, containerId string) {
	if err := dockerClient.ContainerRemove(context.Background(), containerId, container.RemoveOptions{Force: true}); err != nil {
		t.Fatalf("could not remove container %v, consider deleting it manually!", err)
	}
}

func CreateKafkaWithKRaftContainer(dockerClient *client.Client) (id string, err error) {
	ctx := context.Background()

	config := &container.Config{
		Image: "apache/kafka:3.7.0",
		ExposedPorts: nat.PortSet{
			"9092": struct{}{},
		},
		Tty: false,
	}

	hostConfig := &container.HostConfig{
		PortBindings: nat.PortMap{
			"9092": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "9092",
				},
			},
		},
	}

	resp, err := dockerClient.ContainerCreate(ctx, config, hostConfig, nil, nil, "kafka")
	if err != nil {
		panic(err)
	}

	if err := dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		panic(err)
	}

	log.Println("WAITING...")

	wg := &sync.WaitGroup{}
	wg.Add(1)
	// fixme
	go func() {
		time.Sleep(7 * time.Second)
		wg.Done()
	}()
	wg.Wait()

	return resp.ID, nil
}
