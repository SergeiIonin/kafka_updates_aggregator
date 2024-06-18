package test

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
	"kafka_updates_aggregator/kafka_merger/infra"
	"kafka_updates_aggregator/kafka_merger/infra/kafka/merger"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	configTplPath string
	valuesPath    string
	configPath    string
	configFile    []byte
	brokers       []string
	kafkaClient   *kafka.Client
	testTopics    []string
)

func init() {
	pwd, _ := os.Getwd()
	configTplPath = fmt.Sprintf("%s/templates/merger_test_config_template.yaml", pwd)
	valuesPath = fmt.Sprintf("%s/test_values.yaml", pwd)
	configPath = fmt.Sprintf("%s/merger_test_config.yaml", pwd)

	configReader := infra.NewConfigReader()

	err := configReader.ReadConfig(configTplPath, valuesPath, configPath)
	if err != nil {
		log.Fatalf("Error creating merger_config: %v", err)
	}
	configFile, err = os.ReadFile(configPath)

	// kafka setup
	brokers = []string{"localhost:9092"}
	kafkaClient = &kafka.Client{
		Addr:      kafka.TCP(brokers[0]),
		Transport: nil,
	}

	_, err = kafkaClient.Heartbeat(context.Background(), &kafka.HeartbeatRequest{
		Addr:            kafka.TCP(brokers[0]),
		GroupID:         "",
		GenerationID:    0,
		MemberID:        "",
		GroupInstanceID: "",
	})
	if err != nil {
		log.Fatal("failed to heartbeat:", err)
	}
	log.Println("Heartbeat is successful")

	// create test topics
	testTopics = []string{"test_1", "test_2", "test_3"}
	kafkaTopics := make([]kafka.TopicConfig, len(testTopics))
	for i, topic := range testTopics {
		kafkaTopics[i] = kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}

	_, err = kafkaClient.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Addr:         kafka.TCP(brokers[0]),
		Topics:       kafkaTopics,
		ValidateOnly: false,
	})
	if err != nil {
		log.Fatalf("failed to create topics: %v, %v", testTopics, err)
	}

}

func TestKafkaMerger_test(t *testing.T) {
	var kafkaMergerConfig infra.KafkaMergerConfig
	err := yaml.Unmarshal(configFile, &kafkaMergerConfig)
	if err != nil {
		log.Fatalf("Error parsing kafkaMergerConfig file: %v", err)
	}
	merger := merger.NewKafkaMerger(kafkaMergerConfig)

	mergedSourceTopic := kafkaMergerConfig.KafkaMerger.MergedSourceTopic
	_, err = kafkaClient.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Addr: kafka.TCP(brokers[0]),
		Topics: []kafka.TopicConfig{
			{
				Topic:             mergedSourceTopic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
		ValidateOnly: false,
	})
	if err != nil {
		log.Fatalf("failed to create merged_source_topic: %s, %v", mergedSourceTopic, err)
	}

	writeToTopics(testTopics)

	name := "kafka_merge_test"

	t.Run(name, func(t *testing.T) {
		config := merger.ReaderConfig("test_1")
		reader := kafka.NewReader(config)
		msgs := make([]kafka.Message, 50)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			select {
			case <-time.After(15 * time.Second):
				t.Logf("Test finished")
				t.Logf("Messages: %v", msgs)
				cancel()
				return
			}
		}()

		go merger.ConsumeTopic(reader, msgs, ctx)
		t.Fatal("Test failed")
	})
}

func writeToTopics(topics []string) {
	wg := &sync.WaitGroup{}
	wg.Add(len(topics))
	for _, t := range topics {
		w := kafka.Writer{
			Addr:  kafka.TCP(brokers[0]),
			Topic: t,
		}
		go writeToTopic(&w, wg)
	}
	wg.Wait()
}

func writeToTopic(w *kafka.Writer, wg *sync.WaitGroup) {
	for i := 0; i < 5; i++ {
		millisRand := rand.Intn(50)
		time.Sleep(time.Duration(millisRand))
		topic := w.Topic
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%s-%d", topic, i)),
			Value: []byte(fmt.Sprintf("Value-%s-%d", topic, i)),
		}
		err := w.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Printf("Error writing message to topic %s: %v", topic, err)
		}
	}
	wg.Done()
	defer func() {
		err := w.Close()
		if err != nil {
			log.Printf("Error closing writer: %v", err)
		}
	}()
}
