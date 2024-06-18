package testutils

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

type KafkaTestReader struct {
	*kafka.Reader
}

func (ktr *KafkaTestReader) Read(messages []kafka.Message) (err error) {
	for i := range messages {
		m, err := ktr.ReadMessage(context.Background())
		if err != nil {
			log.Printf("could not read message %v", err)
			return err
		}
		messages[i] = m
	}
	return
}
