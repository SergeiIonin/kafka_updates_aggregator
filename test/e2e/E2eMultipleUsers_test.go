package e2e

import (
    "context"
    "encoding/json"
    "fmt"
    "github.com/docker/docker/client"
    "github.com/redis/go-redis/v9"
    "github.com/segmentio/kafka-go"
    "github.com/stretchr/testify/assert"
    "kafka_updates_aggregator/kafka_aggregator"
    "kafka_updates_aggregator/kafka_aggregator/fieldscache"
    "kafka_updates_aggregator/kafka_aggregator/schemasreader"
    "kafka_updates_aggregator/kafka_merger"
    kafkaschemashandler "kafka_updates_aggregator/kafka_schemas_handler"
    "kafka_updates_aggregator/kafka_schemas_handler/schemaswriter"
    "kafka_updates_aggregator/test"
    "kafka_updates_aggregator/test/e2e/e2eutils"
    "log"
    "slices"
    "testing"
    "time"
)

var (
	kafkaBroker                = "localhost:9092"
	kafkaAddr                  = kafka.TCP(kafkaBroker)
	kafkaClient                *kafka.Client
	kafkaContainerId           string
	sourceTopics               = []string{"user_login", "user_deposit", "user_withdrawal"}
	mergedSourcesTopic         = "test_merged"
	aggregatedTopics           = []string{"aggregated_user_balance_updates", "aggregated_user_login_info"}
	numExpectedMessages        = 12
	topicToNumExpectedMessages = map[string]int{
		"aggregated_user_balance_updates": 6,
		"aggregated_user_login_info":      6,
	}

	schemasTopic = "_schemas"

	redisAddr          string
	redisContainerId   string
	schemasRedisReader *schemasreader.SchemasRedisReader
	schemasRedisWriter *schemaswriter.SchemasRedisWriter
	redisClient        *redis.Client
	fieldsRedisCache   *fieldscache.FieldsRedisCache

	dockerClient *client.Client
	startTimeout = 10 * time.Second
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
		Addr:   kafkaAddr,
		Topics: topicConfigs,
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
		case <- timer.C:
			panic(fmt.Sprintf("Kafka and Redis haven't initialized within %v", startTimeout))
		}
		break
	}

	schemasRedisWriter = schemaswriter.NewSchemasRedisWriter(redisAddr)
	schemasRedisReader = schemasreader.NewSchemasRedisReader(redisAddr)
	fieldsRedisCache = fieldscache.NewFieldsRedisCache(redisAddr)
}

func cleanup() {
	if err := test.TerminateContainer(dockerClient, kafkaContainerId); err != nil {
		log.Fatalf(err.Error())
	}
	if err := test.TerminateContainer(dockerClient, redisContainerId); err != nil {
		log.Fatalf(err.Error())
	}
}

func Test_e2eMultipleUsers_test(t *testing.T) {

	schemasHandler := kafkaschemashandler.NewKafkaSchemasHandler(kafkaBroker, schemasRedisWriter)

	kafkaMerger := kafka_merger.NewKafkaMerger([]string{kafkaBroker}, sourceTopics, "e2e-group", mergedSourcesTopic)

	aggregator := kafka_aggregator.NewKafkaAggregator(kafkaBroker, mergedSourcesTopic, schemasRedisReader, fieldsRedisCache)

	t.Run("get aggregated records for the fields from the source topics according to the schemas", func(t *testing.T) {
		t.Cleanup(cleanup)
		ctx := context.Background()

		// TEST DATA
		schemas := []e2eutils.SchemaMsg{
			{
				Key:   []byte(fmt.Sprintf("{\"keytype\":\"SCHEMA\",\"subject\":\"%s\",\"version\":%d,\"magic\":1}", "aggregated_user_balance_updates", 1)),
				Value: []byte(fmt.Sprintf("{\"subject\":\"%s\",\"version\":%d,\"id\":1,\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"aggregated_user_balance_updates\\\",\\\"fields\\\":[{\\\"name\\\":\\\"deposit\\\",\\\"type\\\":\\\"int\\\"},{\\\"name\\\":\\\"withdrawal\\\",\\\"type\\\":\\\"int\\\"},{\\\"name\\\":\\\"balance\\\",\\\"type\\\":\\\"int\\\"}]}\",\"deleted\":false}", "aggregated_user_balance_updates", 1)),
			},
			{
				Key:   []byte(fmt.Sprintf("{\"keytype\":\"SCHEMA\",\"subject\":\"%s\",\"version\":%d,\"magic\":1}", "aggregated_user_login_info", 1)),
				Value: []byte(fmt.Sprintf("{\"subject\":\"%s\",\"version\":%d,\"id\":1,\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"aggregated_user_login_info\\\",\\\"fields\\\":[{\\\"name\\\":\\\"login_time\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"balance\\\",\\\"type\\\":\\\"int\\\"}]}\",\"deleted\":false}", "aggregated_user_login_info", 1)),
			},
		}

		messsagesWithId := []e2eutils.MessageWithId{
			{
				IdKey:   "user_id",
				IdValue: e2eutils.Bob,
				Message: e2eutils.NewLogin("2021-01-01 12:00:00"),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.John,
				Message: e2eutils.NewLogin("2021-02-01 12:30:00"),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.Bob,
				Message: e2eutils.NewDeposit(1000, 100, true, "Cordovia"),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.John,
				Message: e2eutils.NewDeposit(2000, 200, true, "Nowherestan"),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.Bob,
				Message: e2eutils.NewWithdrawal(150),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.John,
				Message: e2eutils.NewWithdrawal(250),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.Bob,
				Message: e2eutils.NewLogin("2021-01-01 13:00:00"),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.John,
				Message: e2eutils.NewLogin("2021-02-01 13:30:00"),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.Bob,
				Message: e2eutils.NewDeposit(950, 300, true, "Cordovia"),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.John,
				Message: e2eutils.NewDeposit(1950, 400, true, "Nowherestan"),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.Bob,
				Message: e2eutils.NewWithdrawal(350),
			},
			{
				IdKey:   "user_id",
				IdValue: e2eutils.John,
				Message: e2eutils.NewWithdrawal(450),
			},
		}

		// WRITE MESSAGES TO SOURCE TOPICS
		go e2eutils.WriteMessagesForMultipleIdsToSourceTopics(t, kafkaAddr, messsagesWithId, 50*time.Millisecond)

		// MERGE MESSAGES INTO SINGLE TOPIC
		go kafkaMerger.Run(ctx)

		// WRITE SCHEMAS
		e2eutils.WriteSchemas(t, kafkaAddr, schemasTopic, schemas)

		// PROCESS SCHEMAS
		e2eutils.ProcessSchemas(t, schemasHandler, redisClient, aggregatedTopics)

		// AGGREGATE NEW MESSAGES ACCORDING TO SCHEMAS
		go aggregator.Run(ctx)

		// READ AGGREGATED MESSAGES INTO SEPARATE CHANNELS
		aggregatedMsgsChans := e2eutils.ReadAggregatedMessages(t, []string{kafkaBroker}, topicToNumExpectedMessages)

		t.Logf("[E2E Test] Reading aggregations")

		// COLLECT AGGREGATIONS FOR MULTIPLE USERS
		aggregatesBalanceUpdates, aggregatesLoginInfo := CollectAggregationsForMultipleUsers(t, aggregatedMsgsChans)

		// ASSERT AGGREGATIONS
		assertAggregations(t, aggregatesBalanceUpdates, aggregatesLoginInfo)
	})
}

func CollectAggregationsForMultipleUsers(t *testing.T, aggregatedMsgsChans []chan []kafka.Message) (aggregatesBalanceUpdates map[string]map[e2eutils.BalanceUpdates]int, aggregatesLoginInfo map[string]map[e2eutils.LoginInfo]int) {
	aggregatesBalanceUpdates = make(map[string]map[e2eutils.BalanceUpdates]int)
	aggregatesLoginInfo = make(map[string]map[e2eutils.LoginInfo]int)

	aggregatesBalanceUpdatesBob := make(map[e2eutils.BalanceUpdates]int)
	aggregatesBalanceUpdates[e2eutils.Bob] = aggregatesBalanceUpdatesBob
	aggregatesLoginInfoBob := make(map[e2eutils.LoginInfo]int)
	aggregatesLoginInfo[e2eutils.Bob] = aggregatesLoginInfoBob

	aggregatesBalanceUpdatesJohn := make(map[e2eutils.BalanceUpdates]int)
	aggregatesBalanceUpdates[e2eutils.John] = aggregatesBalanceUpdatesJohn
	aggregatesLoginInfoJohn := make(map[e2eutils.LoginInfo]int)
	aggregatesLoginInfo[e2eutils.John] = aggregatesLoginInfoJohn

	aggregatedMsgs := make([]kafka.Message, 0, numExpectedMessages)

	for _, ch := range aggregatedMsgsChans {
		msgs := <-ch
		aggregatedMsgs = append(aggregatedMsgs, msgs...)
	}

	for i, msg := range aggregatedMsgs {
		k := string(msg.Key)
		if msg.Topic == "aggregated_user_balance_updates" {
			var balanceUpdates e2eutils.BalanceUpdates
			err := json.Unmarshal(msg.Value, &balanceUpdates)
			if err != nil {
				t.Fatal(err)
			}
			if k == e2eutils.Bob {
				aggregatesBalanceUpdates[e2eutils.Bob][balanceUpdates] = i
			} else if k == e2eutils.John {
				aggregatesBalanceUpdates[e2eutils.John][balanceUpdates] = i
			} else {
				t.Fatalf("Unknown key %s", k)
			}
		} else if msg.Topic == "aggregated_user_login_info" {
			var loginInfo e2eutils.LoginInfo
			err := json.Unmarshal(msg.Value, &loginInfo)
			if err != nil {
				t.Fatal(err)
			}
			if k == e2eutils.Bob {
				aggregatesLoginInfo[e2eutils.Bob][loginInfo] = i
			} else if k == e2eutils.John {
				aggregatesLoginInfo[e2eutils.John][loginInfo] = i
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

func assertAggregations(t *testing.T, aggregatesBalanceUpdates map[string]map[e2eutils.BalanceUpdates]int, aggregatesLoginInfo map[string]map[e2eutils.LoginInfo]int) {
	testDataBob := e2eutils.TestData{
		BalanceUpdatesExpected: []e2eutils.BalanceUpdates{
			e2eutils.NewBalanceUpdates(1000, 100, 150),
			e2eutils.NewBalanceUpdates(950, 300, 150),
			e2eutils.NewBalanceUpdates(950, 300, 350),
		},
		LoginInfoExpected: []e2eutils.LoginInfo{
			e2eutils.NewLoginInfo("2021-01-01 12:00:00", 1000),
			e2eutils.NewLoginInfo("2021-01-01 13:00:00", 1000),
			e2eutils.NewLoginInfo("2021-01-01 13:00:00", 950),
		},
	}

	testDataJohn := e2eutils.TestData{
		BalanceUpdatesExpected: []e2eutils.BalanceUpdates{
			e2eutils.NewBalanceUpdates(2000, 200, 250),
			e2eutils.NewBalanceUpdates(1950, 400, 250),
			e2eutils.NewBalanceUpdates(1950, 400, 450),
		},
		LoginInfoExpected: []e2eutils.LoginInfo{
			e2eutils.NewLoginInfo("2021-02-01 12:30:00", 2000),
			e2eutils.NewLoginInfo("2021-02-01 13:30:00", 2000),
			e2eutils.NewLoginInfo("2021-02-01 13:30:00", 1950),
		},
	}

	offsetsBalanceUpdatesBob := make([]int, 0, len(testDataBob.BalanceUpdatesExpected))
	for _, bu := range testDataBob.BalanceUpdatesExpected {
		o, ok := aggregatesBalanceUpdates[e2eutils.Bob][bu]
		t.Logf("%v written to aggregate for %s is %v", bu, e2eutils.Bob, ok)
		assert.Equal(t, true, ok)
		offsetsBalanceUpdatesBob = append(offsetsBalanceUpdatesBob, o)
	}
	assert.Equal(t, true, slices.IsSorted(offsetsBalanceUpdatesBob))

	offsetsBalanceUpdatesJohn := make([]int, 0, len(testDataJohn.BalanceUpdatesExpected))
	for _, bu := range testDataJohn.BalanceUpdatesExpected {
		o, ok := aggregatesBalanceUpdates[e2eutils.John][bu]
		t.Logf("%v written to aggregate for %s is %v", bu, e2eutils.John, ok)
		assert.Equal(t, true, ok)
		offsetsBalanceUpdatesJohn = append(offsetsBalanceUpdatesJohn, o)
	}
	assert.Equal(t, true, slices.IsSorted(offsetsBalanceUpdatesJohn))

	offsetsLoginInfoBob := make([]int, 0, len(testDataBob.LoginInfoExpected))
	for _, li := range testDataBob.LoginInfoExpected {
		o, ok := aggregatesLoginInfo[e2eutils.Bob][li]
		t.Logf("%v written to aggregate for %s is %v", li, e2eutils.Bob, ok)
		assert.Equal(t, true, ok)
		offsetsLoginInfoBob = append(offsetsLoginInfoBob, o)
	}
	assert.Equal(t, true, slices.IsSorted(offsetsLoginInfoBob))

	offsetsLoginInfoJohn := make([]int, 0, len(testDataJohn.LoginInfoExpected))
	for _, li := range testDataJohn.LoginInfoExpected {
		o, ok := aggregatesLoginInfo[e2eutils.John][li]
		t.Logf("%v written to aggregate for %s is %v", li, e2eutils.John, ok)
		assert.Equal(t, true, ok)
		offsetsLoginInfoJohn = append(offsetsLoginInfoJohn, o)
	}
	assert.Equal(t, true, slices.IsSorted(offsetsLoginInfoJohn))
}
