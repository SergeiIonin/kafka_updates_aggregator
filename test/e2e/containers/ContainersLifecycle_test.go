package containers

import (
	"context"
	"fmt"
	"kafka_updates_aggregator/test"
	"log"
	"testing"
	"time"

	"github.com/docker/docker/client"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

var (
	kafkaBroker        = "localhost:9092"
	kafkaAddr          = kafka.TCP(kafkaBroker)
	kafkaClient        *kafka.Client
	kafkaContainerId   string
	sourceTopics       = []string{"user_login", "user_deposit", "user_withdrawal"}
	mergedSourcesTopic = "test_merged"
	aggregatedTopics   = []string{"aggregated_user_balance_updates", "aggregated_user_login_info"}

	schemasTopic = "_schemas"

	redisAddr        string
	redisContainerId string

	dockerClient *client.Client
	startTimeout time.Duration = 10 * time.Second
)

func initDocker() {
	var err error = nil
	dockerClient, err = client.NewClientWithOpts(client.WithVersion("1.45"))
	if err != nil {
		log.Printf("error creating docker client: %s", err.Error())
		panic(err)
	}
}

func initKafka() (string, error) {
	var err error
	kafkaContainerId, err = test.CreateKafkaWithKRaftContainer(dockerClient)
	if err != nil {
		log.Fatalf("could not create kafka container %v", err)
		return "", err
	}

	log.Printf("KafkaContainerId: %s", kafkaContainerId)

	kafkaClient = &kafka.Client{
		Addr:      kafkaAddr,
		Transport: nil,
	}

	allTopics := append(append(sourceTopics, mergedSourcesTopic, schemasTopic), aggregatedTopics...)

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
		return "", err
	}

	return kafkaContainerId, nil
}

func initRedis() (string, error) {
	var err error
	redisContainerId, err = test.CreateRedisContainer(dockerClient)
	if err != nil {
		log.Fatalf("could not create redis container %v", err)
		return "", err
	}

	log.Printf("RedisContainerId: %s", redisContainerId)

	return redisContainerId, nil
}

func init() {
	chanContainerIds := make(chan string)
	errorsChan := make(chan error)
	runTask := func(task func() (string, error)) {
		id, taskErr := task()
		if taskErr != nil {
			errorsChan <- taskErr
			return
		}
		chanContainerIds <- id
	}
	initDocker()
	ids := make([]string, 0, 2)

	go runTask(initKafka)
	go runTask(initRedis)

	timer := time.NewTimer(startTimeout)

	for {
		timer.Reset(startTimeout)
		select {
		case id := <-chanContainerIds:
			ids = append(ids, id)
			if len(ids) == 2 {
				break
			}
			continue
		case e := <-errorsChan:
			panic(fmt.Sprintf("Kafka and Redis haven't initialized due to error %v", e))
		case <-timer.C:
			panic(fmt.Sprintf("Kafka and Redis haven't initialized within %v", startTimeout))
		}
		break
	}
}

func Test_ContainersLifecycle_test(t *testing.T) {

	assert.Equal(t, 1, 1)

	defer func() {
		if err := test.TerminateContainer(dockerClient, kafkaContainerId); err != nil {
			t.Fatalf(err.Error())
		}
		if err := test.TerminateContainer(dockerClient, redisContainerId); err != nil {
			t.Fatalf(err.Error())
		}
	}()

}
