package singleuser

import (
	"context"
	"fmt"
	"github.com/docker/docker/client"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"kafka_updates_aggregator/e2e/e2eutils"
	"kafka_updates_aggregator/infra"
	"kafka_updates_aggregator/kafka_aggregator"
	"kafka_updates_aggregator/kafka_aggregator/fieldscache"
	"kafka_updates_aggregator/kafka_aggregator/schemasreader"
	"kafka_updates_aggregator/kafka_merger/infra/kafka/merger"
	"kafka_updates_aggregator/kafka_schemas_handler/handler"
	"kafka_updates_aggregator/kafka_schemas_handler/schemaswriter"
	"kafka_updates_aggregator/testutils"
	"log"
	"slices"
	"sync"
	"testing"
	"time"
)

var (
	kafkaBroker         = "localhost:9092"
	kafkaAddr           = kafka.TCP(kafkaBroker)
	kafkaClient         *kafka.Client
	kafkaContainerId    string
	sourceTopics        = []string{"user_login", "user_deposit", "user_withdrawal"}
	mergedSourcesTopic  = "test_merged"
	aggregatedTopics    = []string{"aggregated_user_balance_updates", "aggregated_user_login_info"}
	numExpectedMessages = 6

	schemasTopic = "_schemas"

	redisAddr          string
	redisContainerId   string
	schemasRedisReader *schemasreader.SchemasRedisReader
	schemasRedisWriter *schemaswriter.SchemasRedisWriter
	redisClient        *redis.Client
	redisPrefixes      infra.RedisPrefixes
	fieldsRedisCache   *fieldscache.FieldsRedisCache

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
	kafkaContainerId, err = testutils.CreateKafkaWithKRaftContainer(dockerClient)
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
	redisContainerId, err = testutils.CreateRedisContainer(dockerClient)
	if err != nil {
		log.Fatalf("could not create redis container %v", err)
		return "", err
	}

	log.Printf("redisContainerId: %s", redisContainerId)
	redisClient = redis.NewClient(&redis.Options{Addr: redisAddr})

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
	for {
		select {
		case id := <-chanContainerIds:
			ids = append(ids, id)
			if len(ids) == 2 {
				break
			}
			continue
		case e := <-errorsChan:
			panic(fmt.Sprintf("Kafka and Redis haven't initialized due to error %v", e))
		case _ = <-time.After(startTimeout):
			panic(fmt.Sprintf("Kafka and Redis haven't initialized within %v", startTimeout))
		}
		break
	}

	redisPrefixes = *infra.NewRedisPrefixes()
	schemasRedisReader = schemasreader.NewSchemasRedisReader(redisAddr, redisPrefixes.FieldPrefix, redisPrefixes.SchemaPrefix)
	schemasRedisWriter = schemaswriter.NewSchemasRedisWriter(redisAddr, redisPrefixes.FieldPrefix, redisPrefixes.SchemaPrefix)
	fieldsRedisCache = fieldscache.NewFieldsRedisCache(redisAddr)
}

func Test_e2eSingleUser_test(t *testing.T) {
	defer func() {
		if err := testutils.TerminateContainer(dockerClient, kafkaContainerId); err != nil {
			t.Fatalf(err.Error())
		}
		if err := testutils.TerminateContainer(dockerClient, redisContainerId); err != nil {
			t.Fatalf(err.Error())
		}
	}()

	schemasHandler := handler.NewKafkaSchemasHandler(kafkaBroker, schemasRedisWriter)

	kafkaMerger := merger.NewKafkaMerger([]string{kafkaBroker}, sourceTopics, "e2e-group", mergedSourcesTopic)

	aggregator := kafka_aggregator.NewKafkaAggregator(kafkaBroker, mergedSourcesTopic, schemasRedisReader, fieldsRedisCache)

	t.Run("get aggregated records for the fields from the source topics according to the schemas", func(t *testing.T) {
		// WRITE SCHEMAS
		wgInit := sync.WaitGroup{}
		wgInit.Add(1)
		go e2eutils.InitSchemas(t, kafkaAddr, schemasTopic, &wgInit)
		wgInit.Wait()

		// PROCESS SCHEMAS
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go testutils.RunWithTimeout(t, "schemaHandler", 30*time.Second, schemasHandler.Run, wg)
		wg.Wait()

		// WRITE MESSAGES TO TOPICS AND AGGREGATE NEW MESSAGES ACCORDING TO SCHEMAS
		wg.Add(3)
		go testutils.RunWithTimeout(t, "kafkaMerger", 60*time.Second, kafkaMerger.Merge, wg)
		go e2eutils.WriteMessagesToSourceTopics(t, kafkaAddr, 50*time.Millisecond, wg)
		go testutils.RunWithTimeout(t, "aggregator", 60*time.Second, aggregator.Listen, wg)
		wg.Wait()

		t.Logf("[E2E Test] Reading aggregations")

		aggregatedMessagesChans := e2eutils.CreateAggregatedMessagesChans(len(aggregatedTopics))
		go e2eutils.ReadAggregatedMessages(t, []string{kafkaBroker}, aggregatedTopics, aggregatedMessagesChans, numExpectedMessages)
		// TEST AGGREGATED MESSAGES
		aggregatedMsgs := e2eutils.CollectAggregatedMsgs(t, aggregatedMessagesChans, numExpectedMessages)
		aggregatesBalanceUpdatesBob, aggregatesLoginInfoBob := e2eutils.CollectAggregationsForUser(t, aggregatedMsgs)

		testData := e2eutils.TestData{
			BalanceUpdatesExpected: []e2eutils.BalanceUpdates{
				e2eutils.NewBalanceUpdates("1000", "100", "150"),
				e2eutils.NewBalanceUpdates("950", "300", "150"),
				e2eutils.NewBalanceUpdates("950", "300", "350"),
			},
			LoginInfoExpected: []e2eutils.LoginInfo{
				e2eutils.NewLoginInfo("2021-01-01 12:00:00", "1000"),
				e2eutils.NewLoginInfo("2021-01-01 13:00:00", "1000"),
				e2eutils.NewLoginInfo("2021-01-01 13:00:00", "950"),
			},
		}

		offsetsBalanceUpdates := make([]int, 0, len(testData.BalanceUpdatesExpected))
		for _, bbu := range testData.BalanceUpdatesExpected {
			o, ok := aggregatesBalanceUpdatesBob[bbu]
			t.Logf("%v written to aggregate for bob is %v", bbu, ok)
			assert.Equal(t, true, ok)
			offsetsBalanceUpdates = append(offsetsBalanceUpdates, o)
		}
		assert.Equal(t, true, slices.IsSorted(offsetsBalanceUpdates))

		offsetsLoginInfo := make([]int, 0, len(testData.LoginInfoExpected))
		for _, bli := range testData.LoginInfoExpected {
			o, ok := aggregatesLoginInfoBob[bli]
			t.Logf("%v written to aggregate for bob is %v", bli, ok)
			assert.Equal(t, true, ok)
			offsetsLoginInfo = append(offsetsLoginInfo, o)
		}
		assert.Equal(t, true, slices.IsSorted(offsetsLoginInfo))
	})
}
