package merger

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
)

type KafkaMerger struct {
	Brokers           []string
	Topics            []string
	GroupId           string
	MergedSourceTopic string
}

func (merger *KafkaMerger) Merge(ctx context.Context) {
	wg := &sync.WaitGroup{}
	wg.Add(len(merger.Topics) * 2)

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

	write := func(writer *kafka.Writer, ch <-chan kafka.Message) {
		for {
			select {
			case m, _ := <-ch:
				log.Printf("writing message from topic %s, %s", m.Topic, string(m.Value))
				if err := writer.WriteMessages(ctx, m); err != nil {
					log.Fatalf("failed to write message: %v", err)
				}
			case <-ctx.Done():
				log.Printf("Writer is canceled")
				wg.Done()
				return
			}
		}

		for m := range ch {
			log.Printf("writing message from topic %s, %s", m.Topic, string(m.Value))
			if err := writer.WriteMessages(ctx, m); err != nil {
				log.Fatalf("failed to write message: %v", err)
			}
		}
	}

	read := func(reader *kafka.Reader, ch chan<- kafka.Message) {
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					log.Printf("Reader is canceled")
					wg.Done()
					//close(ch) // fixme the channel will be already closed, why?
					return
				}
				log.Fatalf("failed to read message: %v", err)
			}
			log.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
				m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			m.Headers = append(m.Headers, kafka.Header{
				"initTopic", []byte(m.Topic),
			})
			m.Topic = ""
			ch <- m
		}
	}

	readers := make([]*kafka.Reader, len(merger.Topics))
	writers := make([]*kafka.Writer, len(merger.Topics))
	chans := make([]chan kafka.Message, len(merger.Topics))
	for i, topic := range merger.Topics {
		readers[i] = createReader(topic, fmt.Sprintf("%s_%d", merger.GroupId, i))
		writers[i] = createWriter(merger.MergedSourceTopic)
		chans[i] = make(chan kafka.Message, 100)
	}

	// messages from the same topic will be ordered in the merged topic

	for i, _ := range merger.Topics {
		go write(writers[i], chans[i])
		go read(readers[i], chans[i])
	}

	wg.Wait()
	log.Printf("Exiting merge")
}
