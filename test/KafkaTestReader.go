package test

import (
	"context"
	"kafka_updates_aggregator/domain"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaTestReader struct {
	*kafka.Reader
}

func (ktr *KafkaTestReader) Read(expected int, count *int) ([]kafka.Message, error) {
	log.Printf("[KafkaTestReader] reading messages")
	messages := make([]kafka.Message, 0, expected)

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[KafkaTestReader] context cancelled")
			cancel()
			return messages, ctx.Err()
		default:
			m, err := ktr.ReadMessage(ctx)
			log.Printf("[KafkaTestReader] read message %s", string(m.Value))
			if err != nil {
				if domain.ContextCanceledOrDeadlineExceeded(err) {
					log.Printf("[KafkaTestReader] context cancelled")
					return messages, err
				}
				log.Printf("[KafkaTestReader] could not read message %v", err)
			}
			messages = append(messages, m)
			*count++
			if *count == expected {
				cancel()
				return messages, nil
			}
		}
	}
}

func (ktr *KafkaTestReader) ReadPlain(expected int, count *int) ([]kafka.Message, error) {
	log.Printf("[KafkaTestReader] reading messages")
	messages := make([]kafka.Message, 0, expected)

	for {
		m, err := ktr.ReadMessage(context.Background())
		log.Printf("[KafkaTestReader] read message %s", string(m.Value))
		if err != nil {
			log.Fatalf("[KafkaTestReader] could not read message %v", err)
		}
		messages = append(messages, m)
		*count++
		if *count == expected {
			return messages, nil
		}
	}
}

func (ktr *KafkaTestReader) ReadWithContext(ctx context.Context, cancel context.CancelFunc, expected int, count *int) ([]kafka.Message, error) {
	log.Printf("[KafkaTestReader] reading messages")
	messages := make([]kafka.Message, 0, expected)

	for {
		m, err := ktr.ReadMessage(ctx)
		log.Printf("[KafkaTestReader] read message %s", string(m.Value))
		if err != nil {
			log.Fatalf("[KafkaTestReader] could not read message %v", err)
		}
		messages = append(messages, m)
		*count++
		if *count == expected {
			cancel()
			return messages, nil
		}
	}
}
