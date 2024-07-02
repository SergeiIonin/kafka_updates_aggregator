package merger

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
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

func (merger *KafkaMerger) Merge(ctx context.Context) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(merger.Brokers...),
		Topic:    merger.MergedSourceTopic,
		Balancer: &kafka.LeastBytes{},
	}

	batchBuffer := NewBatchBuffer(500*time.Millisecond, 10, writer)

	contextOrDeadlineExceeded := func(err error) bool {
		return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
	}

	createReader := func(topic string, groupId string) (reader *kafka.Reader) {
		config := kafka.ReaderConfig{
			Brokers:     merger.Brokers,
			Topic:       topic,
			GroupID:     groupId, // fixme
			MinBytes:    10e3,    // 10KB
			MaxBytes:    10e6,    // 10MB
			StartOffset: kafka.FirstOffset,
		}
		return kafka.NewReader(config)
	}

	write := func(ch <-chan kafka.Message) {
		if err := batchBuffer.Run(ctx, ch); err != nil {
			if contextOrDeadlineExceeded(err) {
				return
			}
			log.Fatalf("[KafkaMerger] failed to write message: %v", err)
		}
	}

	read := func(reader *kafka.Reader, ch chan<- kafka.Message) {
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if contextOrDeadlineExceeded(err) {
					log.Printf("[KafkaMerger] Reader is canceled")
					//close(ch) // todo how to close the channel properly?
					return
				}
				log.Printf("[KafkaMerger] failed to read message: %v", err) // fixme
			}
			log.Printf("[KafkaMerger] message at topic/partition/offset %v/%v/%v: %s = %s\n",
				m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			m.Headers = append(m.Headers, kafka.Header{
				"initTopic", []byte(m.Topic),
			})
			m.Topic = ""
			ch <- m
		}
	}

	/*readers := make([]*kafka.Reader, len(merger.Topics))
	writers := make([]*kafka.Writer, len(merger.Topics))
	chans := make([]chan kafka.Message, len(merger.Topics))

	for i, topic := range merger.Topics {
		readers[i] = createReader(topic, fmt.Sprintf("%s_%d", merger.GroupId, i))
		writers[i] = createWriter(merger.MergedSourceTopic)
		chans[i] = make(chan kafka.Message, 100)
	}

	// messages from the same topic will be ordered in the merged topic

	for i := range merger.Topics {
		go write(writers[i], chans[i])
		go read(readers[i], chans[i])
	}

	wg.Wait()
	log.Printf("[KafkaMerger] Exiting merge")*/

	ch := make(chan kafka.Message)
	readers := make([]*kafka.Reader, len(merger.Topics))

	for i, topic := range merger.Topics {
		readers[i] = createReader(topic, fmt.Sprintf("%s_%d", merger.GroupId, i))
	}

	for i := range merger.Topics {
		go read(readers[i], ch)
	}

	go write(ch)

	log.Printf("[KafkaMerger] Exiting merge")
}
