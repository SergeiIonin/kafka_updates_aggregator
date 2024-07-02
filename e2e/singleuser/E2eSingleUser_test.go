package singleuser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/docker/docker/client"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"kafka_updates_aggregator/e2e"
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
	kafkaBroker        = "localhost:9092"
	kafkaAddr          = kafka.TCP(kafkaBroker)
	kafkaClient        *kafka.Client
	kafkaContainerId   string
	sourceTopics       = []string{"user_login", "user_deposit", "user_withdrawal"}
	mergedSourcesTopic = "test_merged"
	aggregatedTopics   = []string{"aggregated_user_balance_updates", "aggregated_user_login_info"}

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
	kafkaContainerId, err := testutils.CreateKafkaWithKRaftContainer(dockerClient)
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
	redisContainerId, err := testutils.CreateRedisContainer(dockerClient)
	if err != nil {
		log.Fatalf("could not create kafka container %v", err)
		return "", err
	}

	log.Printf("KafkaContainerId: %s", kafkaContainerId)
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
	/*defer func() {
		terminateCtx := context.Background()
		testutils.TerminateContainer(dockerClient, kafkaContainerId, terminateCtx, t)
		testutils.TerminateContainer(dockerClient, redisContainerId, terminateCtx, t)
	}()*/

	schemasHandler := handler.NewKafkaSchemasHandler(kafkaBroker, schemasRedisWriter)

	kafkaMerger := merger.NewKafkaMerger([]string{kafkaBroker}, sourceTopics, "e2e-group", mergedSourcesTopic)

	aggregator := kafka_aggregator.NewKafkaAggregator(kafkaBroker, mergedSourcesTopic, schemasRedisReader, fieldsRedisCache)

	t.Run("get aggregated records for the fields from the source topics according to the schemas", func(t *testing.T) {
		// WRITE SCHEMAS
		wgInit := sync.WaitGroup{}
		wgInit.Add(1)
		go initSchemas(t, &wgInit)
		wgInit.Wait()

		// PROCESS SCHEMAS
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go testutils.RunWithTimeout(t, "schemaHandler", 30*time.Second, schemasHandler.Run, wg)
		wg.Wait()

		// WRITE MESSAGES TO TOPICS AND AGGREGATE NEW MESSAGES ACCORDING TO SCHEMAS
		wg.Add(3)
		go testutils.RunWithTimeout(t, "kafkaMerger", 60*time.Second, kafkaMerger.Merge, wg)
		go writeMessagesToSourceTopics(t, 50*time.Millisecond, wg)
		go testutils.RunWithTimeout(t, "aggregator", 60*time.Second, aggregator.Listen, wg)
		wg.Wait()

		t.Logf("[E2E Test] Reading aggregations")

		numExpectedMessages := 6
		aggregatedMessagesChans := createAggregatedMessagesChans(len(aggregatedTopics))
		go readAggregatedMessages(t, []string{kafkaBroker}, aggregatedTopics, aggregatedMessagesChans, numExpectedMessages)
		// TEST AGGREGATED MESSAGES
		aggregatedMsgs := collectAggregatedMsgs(t, aggregatedMessagesChans, numExpectedMessages)
		aggregatesBalanceUpdatesBob, aggregatesLoginInfoBob := collectAggregations(t, aggregatedMsgs)

		testData := TestData{
			BalanceUpdatesExpected: []e2e.BalanceUpdates{
				e2e.NewBalanceUpdates("1000", "100", "150"),
				e2e.NewBalanceUpdates("950", "300", "150"),
				e2e.NewBalanceUpdates("950", "300", "350"),
			},
			LoginInfoExpected: []e2e.LoginInfo{
				e2e.NewLoginInfo("2021-01-01 12:00:00", "1000"),
				e2e.NewLoginInfo("2021-01-01 13:00:00", "1000"),
				e2e.NewLoginInfo("2021-01-01 13:00:00", "950"),
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

type TestData struct {
	BalanceUpdatesExpected []e2e.BalanceUpdates
	LoginInfoExpected      []e2e.LoginInfo
}

func initSchemas(t *testing.T, wg *sync.WaitGroup) {
	ctx := context.Background()
	createSchemaMsgs := []kafka.Message{
		{
			Topic:     schemasTopic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("{\"keytype\":\"SCHEMA\",\"subject\":\"%s\",\"version\":%d,\"magic\":1}", "aggregated_user_balance_updates", 1)),
			Value:     []byte(fmt.Sprintf("{\"subject\":\"%s\",\"version\":%d,\"id\":1,\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"aggregated_user_balance_updates\\\",\\\"fields\\\":[{\\\"name\\\":\\\"deposit\\\",\\\"type\\\":\\\"int\\\"},{\\\"name\\\":\\\"withdrawal\\\",\\\"type\\\":\\\"int\\\"},{\\\"name\\\":\\\"balance\\\",\\\"type\\\":\\\"int\\\"}]}\",\"deleted\":false}", "aggregated_user_balance_updates", 1)),
		},
		{
			Topic:     schemasTopic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("{\"keytype\":\"SCHEMA\",\"subject\":\"%s\",\"version\":%d,\"magic\":1}", "aggregated_user_login_info", 1)),
			Value:     []byte(fmt.Sprintf("{\"subject\":\"%s\",\"version\":%d,\"id\":1,\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"aggregated_user_login_info\\\",\\\"fields\\\":[{\\\"name\\\":\\\"login_time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"balance\\\",\\\"type\\\":\\\"int\\\"}]}\",\"deleted\":false}", "aggregated_user_login_info", 1)),
		},
	}

	kafkaWriter := &kafka.Writer{
		Addr:     kafkaAddr,
		Balancer: &kafka.LeastBytes{},
	}

	err := kafkaWriter.WriteMessages(ctx, createSchemaMsgs...)
	if err != nil {
		t.Fatalf("Error writing createSchemaMsg to _schemas: %v", err)
	}
	if err = kafkaWriter.Close(); err != nil {
		t.Fatalf("Error closing kafka writer: %v", err)
	}
	t.Logf("Schemas written to kafka")
	wg.Done()
}

func createAggregatedMessagesChans(size int) []chan kafka.Message {
	aggregatedMessagesChans := make([]chan kafka.Message, 0, size)
	for i := 0; i < size; i++ {
		aggregatedMessagesChans = append(aggregatedMessagesChans, make(chan kafka.Message))
	}
	return aggregatedMessagesChans
}

func writeMessagesToSourceTopics(t *testing.T, deltaMillis time.Duration, wg *sync.WaitGroup) {
	messsagesWithId := []e2e.MessageWithId{
		{
			IdKey:   "user_id",
			IdValue: "bob",
			Message: e2e.NewLogin("2021-01-01 12:00:00"),
		},
		{
			IdKey:   "user_id",
			IdValue: "bob",
			Message: e2e.NewDeposit(1000, 100, true, "Cordovia"),
		},
		{
			IdKey:   "user_id",
			IdValue: "bob",
			Message: e2e.NewWithdrawal(150),
		},
		{
			IdKey:   "user_id",
			IdValue: "bob",
			Message: e2e.NewLogin("2021-01-01 13:00:00"),
		},
		{
			IdKey:   "user_id",
			IdValue: "bob",
			Message: e2e.NewDeposit(950, 300, true, "Cordovia"),
		},
		{
			IdKey:   "user_id",
			IdValue: "bob",
			Message: e2e.NewWithdrawal(350),
		},
	}

	toKafkaMsg := func(idKey string, idValue string, msg e2e.MessageWithTopic) (kafka.Message, error) {
		id := fmt.Sprintf("{\"%s\":\"%s\"}", idKey, idValue)
		var kafkaMsg kafka.Message
		payload := make([]byte, 0, 10)
		payload, err := json.Marshal(msg)
		if err != nil {
			return kafka.Message{}, err
		}
		kafkaMsg = kafka.Message{
			Topic: msg.Topic(),
			Key:   []byte(id),
			Value: payload,
		}
		return kafkaMsg, nil
	}

	kafkaMessages := make([]kafka.Message, 0, len(messsagesWithId))
	for _, messageWithId := range messsagesWithId {
		kafkaMsg, err := toKafkaMsg(messageWithId.IdKey, messageWithId.IdValue, messageWithId.Message)
		if err != nil {
			t.Fatal(err)
		}
		kafkaMessages = append(kafkaMessages, kafkaMsg)
	}

	kafkaWriter := &kafka.Writer{
		Addr:     kafkaAddr,
		Balancer: &kafka.LeastBytes{},
	}

	for _, kafkaMessage := range kafkaMessages {
		err := kafkaWriter.WriteMessages(context.Background(), kafkaMessage)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(deltaMillis)
	}
	kafkaWriter.Close()
	t.Logf("%d test messages written to kafka", len(kafkaMessages))
	wg.Done()
}

func readAggregatedMessages(t *testing.T, brokers []string, aggregatedTopics []string,
	aggregatedMsgsChans []chan kafka.Message, expectedNumMessages int) {
	mutex := &sync.Mutex{}
	count := 0
	ctx, cancel := context.WithCancel(context.Background())
	readTopic := func(reader *kafka.Reader, ch chan<- kafka.Message, count *int) {
		for {
			msg, err := reader.ReadMessage(ctx)
			t.Logf("[E2E Test] Reading message from aggregated topic %s", msg.Topic)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					t.Logf("[E2E Test] Reading from aggregated topic %s is canceled", msg.Topic) // fixme rm
					t.Logf("[E2E Test] Closing the channel")                                     // fixme rm
					close(ch)
					break
				}
				t.Fatalf("[E2E Test] Error reading from aggregated topic %s %v", msg.Topic, err)
			}
			mutex.Lock()
			*count++
			t.Logf("[E2E Test] Read %d messages from aggregated topics", *count)
			mutex.Unlock()
			ch <- msg
			if *count == expectedNumMessages {
				cancel()
			}
		}
		t.Logf("[E2E Test] readTopic is finished") // fixme rm
		return
	}

	kafkaReaders := make([]*kafka.Reader, 0, len(aggregatedTopics))
	for _, topic := range aggregatedTopics {
		kafkaReaders = append(kafkaReaders, kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			GroupID:     "aggregatedTopicsReader",
			Topic:       topic,
			MinBytes:    10e3, // 10KB
			MaxBytes:    10e6, // 10MB
			StartOffset: kafka.FirstOffset,
		}))
	}

	for i, testReader := range kafkaReaders {
		t.Logf("[E2E Test] Reading aggregated messages from %s", testReader.Config().Topic)
		go readTopic(testReader, aggregatedMsgsChans[i], &count)
	}
}

func collectAggregatedMsgs(t *testing.T, aggregatedMsgsChans []chan kafka.Message, aggregatedMsgsExpected int) []kafka.Message {
	count := 0
	aggregatedMsgs := make([]kafka.Message, 0, aggregatedMsgsExpected)
	wg := &sync.WaitGroup{}
	wg.Add(len(aggregatedMsgsChans))
	for _, aggrChan := range aggregatedMsgsChans {
		go func() {
			for msg := range aggrChan {
				t.Logf("[E2E Test] Aggregated message from the chan: %s", string(msg.Value))
				aggregatedMsgs = append(aggregatedMsgs, msg)
				count++
				t.Logf("[E2E Test] Aggregated messages count: %d", count)
			}
			t.Logf("[E2E Test] Reading from chan is done") // fixme rm
			wg.Done()
			return
		}()
	}
	wg.Wait()
	return aggregatedMsgs
}

func collectAggregations(t *testing.T, aggregatedMsgs []kafka.Message) (aggregatesBalanceUpdatesBob map[e2e.BalanceUpdates]int, aggregatesLoginInfoBob map[e2e.LoginInfo]int) {
	aggregatesBalanceUpdatesBob = make(map[e2e.BalanceUpdates]int)
	aggregatesLoginInfoBob = make(map[e2e.LoginInfo]int)

	for i, msg := range aggregatedMsgs {
		k := string(msg.Key)
		if msg.Topic == "aggregated_user_balance_updates" {
			var balanceUpdates e2e.BalanceUpdates
			err := json.Unmarshal(msg.Value, &balanceUpdates)
			if err != nil {
				t.Fatal(err)
			}
			if k == "bob" {
				aggregatesBalanceUpdatesBob[balanceUpdates] = i
			} else {
				t.Fatalf("Unknown key %s", k)
			}
		} else if msg.Topic == "aggregated_user_login_info" {
			var loginInfo e2e.LoginInfo
			err := json.Unmarshal(msg.Value, &loginInfo)
			if err != nil {
				t.Fatal(err)
			}
			if k == "bob" {
				aggregatesLoginInfoBob[loginInfo] = i
			} else {
				t.Fatalf("Unknown key %s", k)
			}
		} else {
			t.Fatalf("Unknown topic %s", msg.Topic)
		}
		v := string(msg.Value)
		t.Logf("Aggregated message %d, %s: %s", i, k, v)
	}
	return
}
