package kafka_merger

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/domain"
	"log"
)

type KafkaMerger struct {
	Brokers           []string
	Topics            []string
	GroupId           string
	MergedSourceTopic string
}

func NewKafkaMerger(brokers []string, topics []string, groupId string, mergedSourceTopic string) *KafkaMerger {
	return &KafkaMerger{Brokers: brokers, Topics: topics, GroupId: groupId, MergedSourceTopic: mergedSourceTopic}
}

func createTopicReader(brokers []string, topic string, groupId string) *kafka.Reader {
	config := kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupId,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.FirstOffset,
	}
	return kafka.NewReader(config)
}

func readTopic(ctx context.Context, reader *kafka.Reader, ch chan<- kafka.Message) {
	defer func() {
		err := reader.Close()
		if err != nil {
			log.Printf("[KafkaMerger] failed to close reader: %v", err)
			return
		}
	}()
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if domain.ContextOrDeadlineExceeded(err) {
				log.Printf("[KafkaMerger] Reader is canceled")
			}
			close(ch)
			return
			log.Printf("[KafkaMerger] failed to read message: %v", err)
		}
		log.Printf("[KafkaMerger] message at topic/partition/offset %v/%v/%v: %s = %s\n",
			m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		m.Headers = append(m.Headers,
			kafka.Header{"initTopic", []byte(m.Topic)},
			kafka.Header{"time", []byte(fmt.Sprintf("%d", m.Time.UnixMilli()))},
		)
		m.Topic = ""
		ch <- m
	}
}

func writeToMergedChan(ctx context.Context, chansMerger ChannelsMerger, output chan<- kafka.Message, inputs []chan kafka.Message) {
	if err := chansMerger.Merge(ctx, output, inputs); err != nil {
		if domain.ContextOrDeadlineExceeded(err) {
			return
		}
		log.Fatalf("[KafkaMerger] failed to write message: %v", err)
	}
}

func (merger *KafkaMerger) readTopics(ctx context.Context) []chan kafka.Message {
	readers := make([]*kafka.Reader, len(merger.Topics))
	for i, topic := range merger.Topics {
		readers[i] = createTopicReader(merger.Brokers, topic, fmt.Sprintf("%s_%d", merger.GroupId, i))
	}

	inputChans := make([]chan kafka.Message, 0, len(merger.Topics))
	for _, _ = range merger.Topics {
		inputChans = append(inputChans, make(chan kafka.Message))
	}

	for i := range merger.Topics {
		go readTopic(ctx, readers[i], inputChans[i])
	}

	return inputChans
}

func (merger *KafkaMerger) writeToMergedTopic(ctx context.Context,
	outputChan chan kafka.Message, inputChans []chan kafka.Message) {

	chansMerger := ChannelsMerger{}

	go writeToMergedChan(ctx, chansMerger, outputChan, inputChans)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(merger.Brokers[0]),
		Topic:    merger.MergedSourceTopic,
		Balancer: &kafka.LeastBytes{},
	}

	for msg := range outputChan {
		err := writer.WriteMessages(ctx, msg)
		if err != nil {
			log.Fatalf("[KafkaMerger] failed to write message to merged topic: %v", err)
		}
	}
}

func (merger *KafkaMerger) Merge(ctx context.Context) {
	log.Println("[KafkaMerger] started")

	inputChans := merger.readTopics(ctx)

	outputChan := make(chan kafka.Message)

	merger.writeToMergedTopic(ctx, outputChan, inputChans)
}
