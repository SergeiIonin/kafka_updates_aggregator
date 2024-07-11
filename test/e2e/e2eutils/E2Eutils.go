package e2eutils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/kafka_schemas_handler/handler"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)
// todo consider splitting the package into multiple packages of cohesive files, e.g. testreader, testwriter, testdata etc
type MessageWithTopic interface {
	Topic() string
}

type MessageWithId struct {
	IdKey   string
	IdValue string
	Message MessageWithTopic
}

type Deposit struct {
	topic           string
	Balance         int    `json:"balance"`
	Deposit         int    `json:"deposit"`
	IsAuthenticated bool   `json:"isAuthenticated"`
	Country         string `json:"country"`
}

func (d Deposit) Topic() string {
	return d.topic
}
func NewDeposit(balance int, deposit int, isAuthenticated bool, country string) Deposit {
	return Deposit{topic: "user_deposit", Balance: balance, Deposit: deposit, IsAuthenticated: isAuthenticated, Country: country}
}

type Login struct {
	topic      string
	Login_time string `json:"login_time"`
}

func (d Login) Topic() string {
	return d.topic
}
func NewLogin(login_time string) Login {
	return Login{topic: "user_login", Login_time: login_time}
}

type Withdrawal struct {
	topic      string
	Withdrawal int `json:"withdrawal"`
}

func (d Withdrawal) Topic() string {
	return d.topic
}
func NewWithdrawal(withdrawal int) Withdrawal {
	return Withdrawal{topic: "user_withdrawal", Withdrawal: withdrawal}
}

type BalanceUpdates struct {
	topic      string
	Balance    int `json:"balance"`    // fixme: should be int
	Deposit    int `json:"deposit"`    // fixme: should be int
	Withdrawal int `json:"withdrawal"` // fixme: should be int
}

func (d BalanceUpdates) Topic() string {
	return d.topic
}
func NewBalanceUpdates(balance int, deposit int, withdrawal int) BalanceUpdates {
	return BalanceUpdates{topic: "", Balance: balance, Deposit: deposit, Withdrawal: withdrawal}
}

type LoginInfo struct {
	topic     string
	LoginTime string `json:"login_time"`
	Balance   int    `json:"balance"` // fixme: should be int
}

func (d LoginInfo) Topic() string {
	return d.topic
}
func NewLoginInfo(loginTime string, balance int) LoginInfo {
	return LoginInfo{topic: "", LoginTime: loginTime, Balance: balance}
}

type TestData struct {
	BalanceUpdatesExpected []BalanceUpdates
	LoginInfoExpected      []LoginInfo
}

type SchemaMsg struct {
	Key   []byte
	Value []byte
}

func WriteSchemas(t *testing.T, kafkaAddr net.Addr, schemasTopic string, schemas []SchemaMsg) {
	wgInit := sync.WaitGroup{}
	wgInit.Add(1)
	go InitSchemas(t, kafkaAddr, schemasTopic, schemas, &wgInit)
	wgInit.Wait()
}

func AreSchemasReady(ctx context.Context, cancel context.CancelFunc, redisClient *redis.Client, aggregatedTopics []string) error {
	for {
		time.Sleep(250 * time.Millisecond)
		ok := true
		for _, topic := range aggregatedTopics {
			r, err := redisClient.Get(ctx, fmt.Sprintf("schema.%s.1", topic)).Result()
			if errors.Is(err, redis.Nil) || r == "" {
				ok = false
				break
			}
			if err != nil {
				log.Printf("Error getting schema for topic %s: %v", topic, err)
				return err
			}
		}
		if ok {
			log.Printf("Schemas are processed")
			cancel()
			return nil
		}
	}
}

func ProcessSchemas(t *testing.T, schemasHandler *handler.KafkaSchemasHandler, redisClient *redis.Client, aggregatedTopics []string) {
	ctxSchema, cancel := context.WithCancel(context.Background())

	go schemasHandler.Run(ctxSchema)
	err := AreSchemasReady(ctxSchema, cancel, redisClient, aggregatedTopics)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func InitSchemas(t *testing.T, kafkaAddr net.Addr, schemasTopic string, schemas []SchemaMsg, wg *sync.WaitGroup) {
	ctx := context.Background()
	createSchemaMsgs := make([]kafka.Message, 0, len(schemas))
	for _, msg := range schemas {
		kafkaMsg := kafka.Message{
			Topic:     schemasTopic,
			Partition: 0,
			Key:       msg.Key,
			Value:     msg.Value,
		}
		createSchemaMsgs = append(createSchemaMsgs, kafkaMsg)
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

func toKafkaMsg(idKey string, idValue string, msg MessageWithTopic) (kafka.Message, error) {
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

const Bob = "bob"
const John = "john"

func WriteMessagesForMultipleIdsToSourceTopics(t *testing.T, kafkaAddr net.Addr, messsagesWithId []MessageWithId, deltaMillis time.Duration) {
	log.Printf("[E2E Test] Writing messages to source topics")

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
		log.Printf("[E2E Test] Writing message %s", string(kafkaMessage.Value))
		err := kafkaWriter.WriteMessages(context.Background(), kafkaMessage)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(deltaMillis)
	}
	if err := kafkaWriter.Close(); err != nil {
		t.Fatalf("Error closing kafka writer: %v", err)
	}
	t.Logf("%d test messages written to kafka", len(kafkaMessages))
}

func readTopic(t *testing.T, reader *kafka.Reader, res []kafka.Message, expectedNumMessages int, msgChan chan []kafka.Message) {
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatalf("Error closing kafka reader: %v", err)
			return
		}
	}()

	topic := reader.Config().Topic

	count := 0
	for {
		msg, err := reader.ReadMessage(context.Background())
		t.Logf("[E2E Test] Reading message from aggregated topic %s", msg.Topic)
		if err != nil {
			t.Fatalf("[E2E Test] Error reading from aggregated topic %s %v", msg.Topic, err)
		}
		count++
		t.Logf("[E2E Test] Read %d messages from aggregated topic %s", count, topic)
		res = append(res, msg)
		if count == expectedNumMessages {
			break
		}
	}
	t.Logf("[E2E Test] readTopic is finished %s", topic) // fixme rm
	msgChan <- res
	close(msgChan)
	return
}

func ReadAggregatedMessages(t *testing.T, brokers []string, mapTopicToMessagesExpected map[string]int) []chan []kafka.Message {
	lenAggregatedTopics := len(mapTopicToMessagesExpected)
	topicToMessages := make(map[string][]kafka.Message)
	kafkaReaders := make([]*kafka.Reader, 0, lenAggregatedTopics)

	messagesChans := make([]chan []kafka.Message, 0, lenAggregatedTopics)
	for i := 0; i < lenAggregatedTopics; i++ {
		messagesChans = append(messagesChans, make(chan []kafka.Message))
	}

	for topic, numMessages := range mapTopicToMessagesExpected {
		topicToMessages[topic] = make([]kafka.Message, 0, numMessages)
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
		topic := testReader.Config().Topic
		t.Logf("[E2E Test] Reading aggregated messages from %s", topic)
		go readTopic(t, testReader, topicToMessages[topic], mapTopicToMessagesExpected[topic], messagesChans[i])
	}

	return messagesChans
}
