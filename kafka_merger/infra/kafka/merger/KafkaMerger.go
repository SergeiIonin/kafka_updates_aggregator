package merger

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/kafka_merger/infra"
	"log"
)

type KafkaMerger struct {
	Config infra.KafkaMergerConfig
	Writer kafka.Writer
}

// todo defer Close of readers and writer, handle errors

func NewKafkaMerger(config infra.KafkaMergerConfig) *KafkaMerger {
	writer := kafka.Writer{
		Addr:  kafka.TCP(config.KafkaMerger.Hostname),
		Topic: config.KafkaMerger.MergedSourceTopic,
	}
	return &KafkaMerger{
		Config: config,
		Writer: writer,
	}
}

func (km *KafkaMerger) ReaderConfig(topic string) kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Brokers:  []string{km.Config.KafkaMerger.Hostname},
		Topic:    topic,
		GroupID:  fmt.Sprintf("merger_group_new_%s", topic), // fixme
		MinBytes: 10e3,                                      // 10KB
		MaxBytes: 10e6,                                      // 10MB
	}
}

func (km *KafkaMerger) consumeTopic(topic string, messageChan chan<- kafka.Message) {
	config := km.ReaderConfig(topic)
	log.Printf("[KAFKA_MERGER] reader config %v", config)
	log.Println("-----------------------")
	reader := kafka.NewReader(config)
	log.Printf("[KAFKA_MERGER] reader info %v", reader.Stats())
	log.Println("-----------------------")
	for {
		log.Printf("[KAFKA_MERGER] consuming new message from topic %s", topic)
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("[KAFKA_MERGER] failed to read message: %v", err)
		}
		log.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
			m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		m.Headers = append(m.Headers, kafka.Header{"origin", []byte(topic)})
		messageChan <- m
	}
	defer reader.Close()
}

func (km *KafkaMerger) WriteToMergedSource(messageChan <-chan kafka.Message) {
	for {
		select {
		case m := <-messageChan:
			if err := km.Writer.WriteMessages(context.Background(), m); err != nil {
				log.Fatal("failed to write message:", err) // todo fatal?
			}
		}
	}
}

func (km *KafkaMerger) Consume() {
	for _, topic := range km.Config.KafkaMerger.SourceTopics {
		log.Printf("[KAFKA_MERGER] consuming topic %s", topic)
		go func(topic string) {
			config := km.ReaderConfig(topic)
			log.Printf("[KAFKA_MERGER] reader config %v", config)
			log.Println("-----------------------")
			reader := kafka.NewReader(config)
			log.Printf("[KAFKA_MERGER] reader info %v", reader.Stats())
			log.Println("-----------------------")
			for {
				log.Printf("[KAFKA_MERGER] consuming new message from topic %s", topic)
				m, err := reader.ReadMessage(context.Background())
				if err != nil {
					log.Fatalf("[KAFKA_MERGER] failed to read message: %v", err)
				}
				log.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
					m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

				m.Headers = append(m.Headers, kafka.Header{"origin", []byte(topic)})
			}
			defer func() {
				err := reader.Close()
				if err != nil {
					log.Printf("error closing reader: %v", err)
				}
			}()
		}(topic)
	}
}

func (km *KafkaMerger) ConsumeTopic(reader *kafka.Reader, msgs []kafka.Message, ctx context.Context) {
	defer func() {
		err := reader.Close()
		if err != nil {
			log.Printf("error closing reader: %v", err)
		}
	}()
	topic := reader.Config().Topic
	count := 0
	for {
		log.Printf("[KAFKA_MERGER] consuming new message from topic %s", topic)
		//m, err := reader.ReadMessage(ctx)
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("[KAFKA_MERGER] failed to read message: %v", err)
			/*if errors.As(err, context.Canceled) {
				log.Printf("[KAFKA_MERGER] context canceled")
				return
			}*/
			return
		}
		err = reader.CommitMessages(ctx, m)
		if err != nil {
			log.Fatalf("[KAFKA_MERGER] failed to commit offset: %v", err)
		}
		log.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
			m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		m.Headers = append(m.Headers, kafka.Header{"origin", []byte(topic)})
		msgs[count] = m
		count++
	}
}

func (k *KafkaMerger) Merge() {
	messageChan := make(chan kafka.Message)
	go k.WriteToMergedSource(messageChan)
	for _, topic := range k.Config.KafkaMerger.SourceTopics {
		log.Printf("[KAFKA_MERGER] consuming topic %s", topic)
		go k.consumeTopic(topic, messageChan)
	}
}
