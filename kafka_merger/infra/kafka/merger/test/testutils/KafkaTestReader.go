package testutils

import (
	"context"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type KafkaTestReader struct {
	*kafka.Reader
}

func (ktr *KafkaTestReader) Read(expected int, count *int) ([]kafka.Message, error) {
	ctx, cancel := context.WithCancel(context.Background())
	messages := make([]kafka.Message, 0, expected)

	go func() {
		select {
		case <-time.After(15 * time.Second): // fixme why so long?
			log.Printf("15 second elapsed")
			cancel()
			return
		}
	}()

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
}
