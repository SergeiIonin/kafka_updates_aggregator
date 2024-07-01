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

func NewKafkaMerger(brokers []string, topics []string, groupId string, mergedSourceTopic string) *KafkaMerger {
	return &KafkaMerger{Brokers: brokers, Topics: topics, GroupId: groupId, MergedSourceTopic: mergedSourceTopic}
}

func (merger *KafkaMerger) Merge(ctx context.Context) {
	wg := &sync.WaitGroup{}
	wg.Add(len(merger.Topics) * 2)

	contextOrDeadlineExceeded := func(err error) bool {
		return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
	}

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

	// fixme: if we check errors.Is(err, context.Canceled), then should we read <-ctx.Done()???
	write := func(writer *kafka.Writer, ch <-chan kafka.Message) {
		for {
			select {
			case m, _ := <-ch:
				log.Printf("[KafkaMerger] writing message from topic %s, %s", m.Topic, string(m.Value))
				if err := writer.WriteMessages(ctx, m); err != nil {
					log.Fatalf("[KafkaMerger] failed to write message: %v", err)
					if contextOrDeadlineExceeded(err) {
						wg.Done()
						//close(ch) // fixme the channel will be already closed, why?
						return
					}
				}
			case <-ctx.Done():
				log.Printf("[KafkaMerger] Writer is canceled")
				wg.Done()
				return
			}
		}

		/*for m := range ch {
			log.Printf("[KafkaMerger] writing message from topic %s, %s", m.Topic, string(m.Value))
			if err := writer.WriteMessages(ctx, m); err != nil {
				log.Fatalf("[KafkaMerger] failed to write message: %v", err)
			}
		}*/
	}

	read := func(reader *kafka.Reader, ch chan<- kafka.Message) {
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if contextOrDeadlineExceeded(err) {
					log.Printf("[KafkaMerger] Reader is canceled")
					wg.Done()
					//close(ch) // fixme the channel will be already closed, why?
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

	readers := make([]*kafka.Reader, len(merger.Topics))
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
	log.Printf("[KafkaMerger] Exiting merge")
}
