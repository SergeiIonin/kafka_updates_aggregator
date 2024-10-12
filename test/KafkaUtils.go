package test

import (
	"context"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/segmentio/kafka-go"
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
	if err := GracefulShutdown(dockerClient, containerId); err != nil {
		t.Fatalf("could not remove container %v, consider deleting it manually!", err)
	}
}

func GracefulShutdown(dockerClient *client.Client, containerId string) error {
	log.Printf("Removing container %s \n", containerId)
	inspection, err := dockerClient.ContainerInspect(context.Background(), containerId)
	if err != nil {
		log.Printf("could not inspect container %v, consider deleting it manually!", err)
		return err
	}

	if !inspection.State.Running {
		return nil
	}

	if err = dockerClient.ContainerRemove(context.Background(), containerId, container.RemoveOptions{Force: true}); err != nil {
		log.Printf("could not remove container %v, consider deleting it manually!", err)
		return err
	}
	return nil
}

func waitKafkaIsUp() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	kafkaClient := &kafka.Client{
		Addr:      kafkaAddr,
		Transport: nil,
	}

	for {
		select {
		case <-ctx.Done():
			cancel()
			return ctx.Err()
		default:
			log.Println("WAITING FOR KAFKA TO BE READY...")
			// for some reason Heartbeat request is not enough: even if it's successful,
			// the client may fail to creata topic
			resp, err := kafkaClient.Metadata(ctx, &kafka.MetadataRequest{
				Addr: kafkaAddr,
			})
			if resp == nil || err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			cancel()
			return nil
		}
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

	if err = waitKafkaIsUp(); err != nil {
		panic(err)
	}

	return resp.ID, nil
}
