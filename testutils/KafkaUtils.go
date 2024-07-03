package testutils

import (
	"context"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/segmentio/kafka-go"
	"log"
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
	dockerClient      *client.Client
)

// This is not an actual offsets from the merged data topic, but the offsets from the original topics, which is
// presented in each message's value as a suffix, e.g. value-test_1-3, where test_1 is the original topic and 3 is the offset
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

	if err = dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		panic(err)
	}

	log.Println("WAITING FOR KAFKA CONTAINER TO START...")

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

func CreateRedisContainer(dockerClient *client.Client) (id string, err error) {
	ctx := context.Background()

	config := &container.Config{
		Image: "redis:latest",
		ExposedPorts: nat.PortSet{
			"6379": struct{}{},
		},
		Tty: false,
	}

	hostConfig := &container.HostConfig{
		PortBindings: nat.PortMap{
			"6379": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "6379",
				},
			},
		},
	}

	resp, err := dockerClient.ContainerCreate(ctx, config, hostConfig, nil, nil, "redis")
	if err != nil {
		panic(err)
	}

	if err = dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		panic(err)
	}

	log.Println("WAITING FOR REDIS CONTAINER TO START...")

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
