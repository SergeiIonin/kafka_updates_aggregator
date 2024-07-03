package e2eutils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"net"
	"sync"
	"testing"
	"time"
)

type MessageWithTopic interface {
	Topic() string
	/*MarshallJSON() ([]byte, error)
	UnmarshallJSON(data []byte) error*/
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
	Balance    string `json:"balance"`    // fixme: should be int
	Deposit    string `json:"deposit"`    // fixme: should be int
	Withdrawal string `json:"withdrawal"` // fixme: should be int
}

func (d BalanceUpdates) Topic() string {
	return d.topic
}
func NewBalanceUpdates(balance string, deposit string, withdrawal string) BalanceUpdates {
	return BalanceUpdates{topic: "", Balance: balance, Deposit: deposit, Withdrawal: withdrawal}
}

type LoginInfo struct {
	topic     string
	LoginTime string `json:"login_time"`
	Balance   string `json:"balance"` // fixme: should be int
}

func (d LoginInfo) Topic() string {
	return d.topic
}
func NewLoginInfo(loginTime string, balance string) LoginInfo {
	return LoginInfo{topic: "", LoginTime: loginTime, Balance: balance}
}

type TestData struct {
	BalanceUpdatesExpected []BalanceUpdates
	LoginInfoExpected      []LoginInfo
}

func InitSchemas(t *testing.T, kafkaAddr net.Addr, schemasTopic string, wg *sync.WaitGroup) {
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

func CreateAggregatedMessagesChans(size int) []chan kafka.Message {
	aggregatedMessagesChans := make([]chan kafka.Message, 0, size)
	for i := 0; i < size; i++ {
		aggregatedMessagesChans = append(aggregatedMessagesChans, make(chan kafka.Message))
	}
	return aggregatedMessagesChans
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

func WriteMessagesToSourceTopics(t *testing.T, kafkaAddr net.Addr, deltaMillis time.Duration, wg *sync.WaitGroup) {
	messsagesWithId := []MessageWithId{
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewLogin("2021-01-01 12:00:00"),
		},
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewDeposit(1000, 100, true, "Cordovia"),
		},
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewWithdrawal(150),
		},
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewLogin("2021-01-01 13:00:00"),
		},
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewDeposit(950, 300, true, "Cordovia"),
		},
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewWithdrawal(350),
		},
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
	if err := kafkaWriter.Close(); err != nil {
		t.Fatalf("Error closing kafka writer: %v", err)
	}
	t.Logf("%d test messages written to kafka", len(kafkaMessages))
	wg.Done()
}

func WriteMessagesForMultipleIdsToSourceTopics(t *testing.T, kafkaAddr net.Addr, deltaMillis time.Duration, wg *sync.WaitGroup) {
	messsagesWithId := []MessageWithId{
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewLogin("2021-01-01 12:00:00"),
		},
		{
			IdKey:   "user_id",
			IdValue: John,
			Message: NewLogin("2021-02-01 12:30:00"),
		},
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewDeposit(1000, 100, true, "Cordovia"),
		},
		{
			IdKey:   "user_id",
			IdValue: John,
			Message: NewDeposit(2000, 200, true, "Nowherestan"),
		},
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewWithdrawal(150),
		},
		{
			IdKey:   "user_id",
			IdValue: John,
			Message: NewWithdrawal(250),
		},
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewLogin("2021-01-01 13:00:00"),
		},
		{
			IdKey:   "user_id",
			IdValue: John,
			Message: NewLogin("2021-02-01 13:30:00"),
		},
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewDeposit(950, 300, true, "Cordovia"),
		},
		{
			IdKey:   "user_id",
			IdValue: John,
			Message: NewDeposit(1950, 400, true, "Nowherestan"),
		},
		{
			IdKey:   "user_id",
			IdValue: Bob,
			Message: NewWithdrawal(350),
		},
		{
			IdKey:   "user_id",
			IdValue: John,
			Message: NewWithdrawal(450),
		},
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
	if err := kafkaWriter.Close(); err != nil {
		t.Fatalf("Error closing kafka writer: %v", err)
	}
	t.Logf("%d test messages written to kafka", len(kafkaMessages))
	wg.Done()
}

func ReadAggregatedMessages(t *testing.T, brokers []string, aggregatedTopics []string,
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

func CollectAggregatedMsgs(t *testing.T, aggregatedMsgsChans []chan kafka.Message, aggregatedMsgsExpected int) []kafka.Message {
	aggregatedMsgs := make([]kafka.Message, 0, aggregatedMsgsExpected)
	wg := &sync.WaitGroup{}
	wg.Add(len(aggregatedMsgsChans))
	for _, aggrChan := range aggregatedMsgsChans {
		go func() {
			for msg := range aggrChan {
				t.Logf("[E2E Test] Aggregated message from the chan: %s", string(msg.Value))
				aggregatedMsgs = append(aggregatedMsgs, msg)
			}
			t.Logf("[E2E Test] Reading from chan is done") // fixme rm
			wg.Done()
			return
		}()
	}
	wg.Wait()
	return aggregatedMsgs
}

func CollectAggregationsForUser(t *testing.T, aggregatedMsgs []kafka.Message) (aggregatesBalanceUpdatesBob map[BalanceUpdates]int, aggregatesLoginInfoBob map[LoginInfo]int) {
	aggregatesBalanceUpdatesBob = make(map[BalanceUpdates]int)
	aggregatesLoginInfoBob = make(map[LoginInfo]int)

	for i, msg := range aggregatedMsgs {
		k := string(msg.Key)
		if msg.Topic == "aggregated_user_balance_updates" {
			var balanceUpdates BalanceUpdates
			err := json.Unmarshal(msg.Value, &balanceUpdates)
			if err != nil {
				t.Fatal(err)
			}
			if k == Bob {
				aggregatesBalanceUpdatesBob[balanceUpdates] = i
			} else {
				t.Fatalf("Unknown key %s", k)
			}
		} else if msg.Topic == "aggregated_user_login_info" {
			var loginInfo LoginInfo
			err := json.Unmarshal(msg.Value, &loginInfo)
			if err != nil {
				t.Fatal(err)
			}
			if k == Bob {
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

func CollectAggregationsForMultipleUsers(t *testing.T, aggregatedMsgs []kafka.Message) (aggregatesBalanceUpdates map[string]map[BalanceUpdates]int, aggregatesLoginInfo map[string]map[LoginInfo]int) {
	aggregatesBalanceUpdates = make(map[string]map[BalanceUpdates]int)
	aggregatesLoginInfo = make(map[string]map[LoginInfo]int)

	aggregatesBalanceUpdatesBob := make(map[BalanceUpdates]int)
	aggregatesBalanceUpdates[Bob] = aggregatesBalanceUpdatesBob
	aggregatesLoginInfoBob := make(map[LoginInfo]int)
	aggregatesLoginInfo[Bob] = aggregatesLoginInfoBob

	aggregatesBalanceUpdatesJohn := make(map[BalanceUpdates]int)
	aggregatesBalanceUpdates[John] = aggregatesBalanceUpdatesJohn
	aggregatesLoginInfoJohn := make(map[LoginInfo]int)
	aggregatesLoginInfo[John] = aggregatesLoginInfoJohn

	for i, msg := range aggregatedMsgs {
		k := string(msg.Key)
		if msg.Topic == "aggregated_user_balance_updates" {
			var balanceUpdates BalanceUpdates
			err := json.Unmarshal(msg.Value, &balanceUpdates)
			if err != nil {
				t.Fatal(err)
			}
			if k == Bob {
				aggregatesBalanceUpdates[Bob][balanceUpdates] = i
			} else if k == John {
				aggregatesBalanceUpdates[John][balanceUpdates] = i
			} else {
				t.Fatalf("Unknown key %s", k)
			}
		} else if msg.Topic == "aggregated_user_login_info" {
			var loginInfo LoginInfo
			err := json.Unmarshal(msg.Value, &loginInfo)
			if err != nil {
				t.Fatal(err)
			}
			if k == Bob {
				aggregatesLoginInfo[Bob][loginInfo] = i
			} else if k == John {
				aggregatesLoginInfo[John][loginInfo] = i
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
