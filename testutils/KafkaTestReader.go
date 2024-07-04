package testutils

import (
	"context"
	"github.com/segmentio/kafka-go"
	"kafka_updates_aggregator/domain"
	"log"
	"time"
)

type KafkaTestReader struct {
	*kafka.Reader
}

func (ktr *KafkaTestReader) Read(expected int, count *int) ([]kafka.Message, error) {
	messages := make([]kafka.Message, 0, expected)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

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
				if domain.ContextOrDeadlineExceeded(err) {
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

	/*read := func() {
		for {
			m, err := ktr.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					log.Printf("[KafkaTestReader] context cancelled")
					return messages, err
				}
				log.Printf("[KafkaTestReader] could not read message %v", err)
			}
			messages = append(messages, m)
			*count++
			if *count == expected {
				return messages, nil
			}
		}
	}*/
}
