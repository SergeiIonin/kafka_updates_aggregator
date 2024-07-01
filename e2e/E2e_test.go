package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/docker/docker/client"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"kafka_updates_aggregator/infra"
	"kafka_updates_aggregator/kafka_aggregator"
	"kafka_updates_aggregator/kafka_aggregator/fieldscache"
	"kafka_updates_aggregator/kafka_aggregator/schemasreader"
	"kafka_updates_aggregator/kafka_merger/infra/kafka/merger"
	"kafka_updates_aggregator/kafka_schemas_handler/handler"
	"kafka_updates_aggregator/kafka_schemas_handler/schemaswriter"
	"kafka_updates_aggregator/testutils"
	"log"
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

func Test_e2e_test(t *testing.T) {
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
		wgInit.Add(2)

		initSchemas := func(ctx context.Context, wg *sync.WaitGroup) {
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
			kafkaWriter.Close()
			t.Logf("Schemas written to kafka")
			wg.Done()
		}

		// WRITE TO SOURCE TOPICS
		writeMsgsToSourceTopics := func(ctx context.Context, wg *sync.WaitGroup) {
			messsagesWithId := []MessageWithId{
				{
					IdKey:   "user_id",
					IdValue: "bob",
					Message: NewLogin("2021-01-01 12:00:00"),
				},
				{
					IdKey:   "user_id",
					IdValue: "john",
					Message: NewLogin("2021-02-01 12:30:00"),
				},
				{
					IdKey:   "user_id",
					IdValue: "bob",
					Message: NewDeposit(1000, 100, true, "Cordovia"),
				},
				{
					IdKey:   "user_id",
					IdValue: "john",
					Message: NewDeposit(2000, 200, true, "Nowherestan"),
				},
				{
					IdKey:   "user_id",
					IdValue: "bob",
					Message: NewWithdrawal(150),
				},
				{
					IdKey:   "user_id",
					IdValue: "john",
					Message: NewWithdrawal(250),
				},
				{
					IdKey:   "user_id",
					IdValue: "bob",
					Message: NewLogin("2021-01-01 13:00:00"),
				},
				{
					IdKey:   "user_id",
					IdValue: "john",
					Message: NewLogin("2021-02-01 13:30:00"),
				},
				{
					IdKey:   "user_id",
					IdValue: "bob",
					Message: NewDeposit(950, 300, true, "Cordovia"),
				},
				{
					IdKey:   "user_id",
					IdValue: "john",
					Message: NewDeposit(1950, 400, true, "Nowherestan"),
				},
				{
					IdKey:   "user_id",
					IdValue: "bob",
					Message: NewWithdrawal(350),
				},
				{
					IdKey:   "user_id",
					IdValue: "john",
					Message: NewWithdrawal(450),
				},
			}

			toKafkaMsg := func(idKey string, idValue string, msg MessageWithTopic) (kafka.Message, error) {
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

			err := kafkaWriter.WriteMessages(context.Background(), kafkaMessages...)
			if err != nil {
				t.Fatal(err)
			}
			kafkaWriter.Close()
			t.Logf("%d test messages written to kafka", len(kafkaMessages))
			wg.Done()
		}

		go initSchemas(context.Background(), &wgInit)
		go writeMsgsToSourceTopics(context.Background(), &wgInit)
		wgInit.Wait()

		// RUN LOGIC
		aggregatedMsgsExpected := 12
		aggregatedMsgs := make([]kafka.Message, 0, aggregatedMsgsExpected)
		aggregatedMsgsChans := make([]chan kafka.Message, 0, len(aggregatedTopics))
		for i := 0; i < len(aggregatedTopics); i++ {
			aggregatedMsgsChans = append(aggregatedMsgsChans, make(chan kafka.Message))
		}
		count := 0

		// base function to control the exeecution of schemas handling, merging, aggregation and reading the aggregations topics
		runWithTimeout := func(name string, timeout time.Duration, f func(ctx context.Context), wg *sync.WaitGroup) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			go f(ctx)
			time.Sleep(timeout) // fixme looking ugly, it's just to make sure f(ctx) had enough time
			go func() {
				select {
				case <-ctx.Done():
					t.Logf("[E2E Test] context for %s is canceled after %v", name, timeout)
					if wg != nil {
						wg.Done()
					}
					cancel()
				}
			}()
		}

		readAggregatedMsgs := func(ctx context.Context) {
			readTopic := func(reader *kafka.Reader, ctx context.Context, ch chan<- kafka.Message) {
				for {
					msg, err := reader.ReadMessage(ctx)
					t.Logf("[E2E Test] Reading message from aggregated topic %s", msg.Topic)
					if err != nil {
						t.Logf("[E2E Test] Error reading from aggregated topic %s %v", msg.Topic, err)
						if errors.Is(err, context.DeadlineExceeded) {
							reader.Close()
							close(ch) // safe bc for each reader we have a separate channel
							return
						}
					}
					ch <- msg
				}
			}

			kafkaReaders := make([]*kafka.Reader, 0, len(aggregatedTopics))
			for _, topic := range aggregatedTopics {
				kafkaReaders = append(kafkaReaders, kafka.NewReader(kafka.ReaderConfig{
					Brokers:  []string{kafkaBroker},
					Topic:    topic,
					MinBytes: 10e3, // 10KB
					MaxBytes: 10e6, // 10MB
				}))
			}

			for i, testReader := range kafkaReaders {
				t.Logf("[E2E Test] Reading aggregated messages from %s", testReader.Config().Topic)
				go readTopic(testReader, ctx, aggregatedMsgsChans[i])
			}
		}

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go runWithTimeout("schemaHandler", 30*time.Second, schemasHandler.Run, nil)
		go runWithTimeout("kafkaMerger", 30*time.Second, kafkaMerger.Merge, wg)
		wg.Wait()
		wg.Add(1)
		go runWithTimeout("aggregator", 30*time.Second, aggregator.Listen, wg)
		wg.Wait()
		go runWithTimeout("readAggregatedMsgs", 30*time.Second, readAggregatedMsgs, nil)

		t.Logf("[E2E Test] Reading aggregations")

		collectAggregatedMsgs := func() {
			for _, aggrChan := range aggregatedMsgsChans {
				for msg := range aggrChan {
					t.Logf("[E2E Test] Aggregated message from the chan: %s", string(msg.Value))
					aggregatedMsgs = append(aggregatedMsgs, msg)
					count++
					t.Logf("[E2E Test] Aggregated messages count: %d", count)
				}
			}
		}

		collectAggregatedMsgs()
		t.Logf("[E2E Test] %d Aggregated messages are collected", count)

		aggregatesBalanceUpdatesBob := make(map[BalanceUpdates]bool)
		aggregatesBalanceUpdatesJohn := make(map[BalanceUpdates]bool)
		aggregatesLoginInfoBob := make(map[LoginInfo]bool)
		aggregatesLoginInfoJohn := make(map[LoginInfo]bool)

		for i, msg := range aggregatedMsgs {
			k := string(msg.Key)
			if msg.Topic == "aggregated_user_balance_updates" {
				var balanceUpdates BalanceUpdates
				err := json.Unmarshal(msg.Value, &balanceUpdates)
				if err != nil {
					t.Fatal(err)
				}
				if k == "john" {
					aggregatesBalanceUpdatesJohn[balanceUpdates] = true
				} else if k == "bob" {
					aggregatesBalanceUpdatesBob[balanceUpdates] = true
				} else {
					t.Fatalf("Unknown key %s", k)
				}
			} else if msg.Topic == "aggregated_user_login_info" {
				var loginInfo LoginInfo
				err := json.Unmarshal(msg.Value, &loginInfo)
				if err != nil {
					t.Fatal(err)
				}
				if k == "john" {
					aggregatesLoginInfoJohn[loginInfo] = true
				} else if k == "bob" {
					aggregatesLoginInfoBob[loginInfo] = true
				} else {
					t.Fatalf("Unknown key %s", k)
				}
			} else {
				t.Fatalf("Unknown topic %s", msg.Topic)
			}
			v := string(msg.Value)
			t.Logf("Aggregated message %d, %s: %s", i, k, v)
		}

		bobBalanceUpdates := []BalanceUpdates{
			/*NewBalanceUpdates("1000", "100", "150"),
			NewBalanceUpdates("950", "300", "150"),*/
			NewBalanceUpdates("950", "300", "350"),
		}

		johnBalanceUpdates := []BalanceUpdates{
			/*NewBalanceUpdates("2000", "200", "250"),
			NewBalanceUpdates("1950", "400", "450"),*/
			NewBalanceUpdates("1950", "400", "450"),
		}

		bobLoginInfo := []LoginInfo{
			NewLoginInfo("2021-02-01 12:00:00", "1000"),
			NewLoginInfo("2021-02-01 13:00:00", "950"),
		}

		johnLoginInfo := []LoginInfo{
			NewLoginInfo("2021-02-01 12:30:00", "2000"),
			NewLoginInfo("2021-02-01 13:30:00", "1950"),
		}

		for _, bbu := range bobBalanceUpdates {
			_, ok := aggregatesBalanceUpdatesBob[bbu]
			t.Logf("%v written to aggregate for bob is %v", bbu, ok)
			assert.Equal(t, true, ok)
		}
		for _, jbu := range johnBalanceUpdates {
			_, ok := aggregatesBalanceUpdatesJohn[jbu]
			t.Logf("%v written to aggregate for john is %v", jbu, ok)
			assert.Equal(t, true, ok)
		}

		for _, bli := range bobLoginInfo {
			_, ok := aggregatesLoginInfoBob[bli]
			t.Logf("%v written to aggregate for bob is %v", bli, ok)
			assert.Equal(t, true, ok)
		}
		for _, jli := range johnLoginInfo {
			_, ok := aggregatesLoginInfoJohn[jli]
			t.Logf("%v written to aggregate for john is %v", jli, ok)
			assert.Equal(t, true, ok)
		}
	})

}
