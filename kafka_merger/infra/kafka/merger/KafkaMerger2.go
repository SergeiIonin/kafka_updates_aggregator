package merger

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
)

type KafkaMerger2 struct {
	Brokers           []string
	Topics            []string
	GroupId           string
	MergedSourceTopic string
}

func (merger *KafkaMerger2) Merge(ctx context.Context, wg *sync.WaitGroup, mut *sync.Mutex, count *int) {
	createReader := func(topic string, groupId string) (reader *kafka.Reader) {
		config := kafka.ReaderConfig{
			Brokers:  merger.Brokers,
			Topic:    topic,
			GroupID:  groupId, // fixme
			MinBytes: 10e3,    // 10KB
			MaxBytes: 10e6,    // 10MB
		}
		return kafka.NewReader(config)
	}

	createWriter := func(topic string) (writer *kafka.Writer) {
		return &kafka.Writer{
			Addr:     kafka.TCP(merger.Brokers[0]),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}
	}

	writer := createWriter(merger.MergedSourceTopic)
	writeMessage := func(writer *kafka.Writer, message kafka.Message) {
		if err := writer.WriteMessages(ctx, message); err != nil {
			log.Fatalf("failed to write message: %v", err)
		}
		mut.Lock()
		*count++
		mut.Unlock()
	}

	readAndWriteToMerged := func(reader *kafka.Reader, writer *kafka.Writer) {
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					log.Printf("Reader is canceled")
					wg.Done()
					return
				}
				wg.Done()
				log.Fatalf("failed to read message: %v", err)
			}
			log.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
				m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			m.Headers = append(m.Headers, kafka.Header{
				"initTopic", []byte(m.Topic),
			})
			m.Topic = ""
			go writeMessage(writer, m)
		}
	}

	readers := make([]*kafka.Reader, len(merger.Topics))
	for i, topic := range merger.Topics {
		readers[i] = createReader(topic, fmt.Sprintf("%s_%d", merger.GroupId, i))
	}

	// messages from the same topic will be ordered in the merged topic
	for _, reader := range readers {
		go readAndWriteToMerged(reader, writer)
	}
}
