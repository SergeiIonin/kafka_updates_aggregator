package testutils

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

type KafkaTestWriter struct {
	*kafka.Writer
}

func (ktw *KafkaTestWriter) MakeMessagesForTopic(topic string, num int) []kafka.Message {
	msgs := make([]kafka.Message, num)

	for i, _ := range msgs {
		key := fmt.Sprintf("key-%s-%d", topic, i)
		value := fmt.Sprintf("value-%s-%d", topic, i)
		m := kafka.Message{
			Topic: topic,
			Key:   []byte(key),
			Value: []byte(value),
		}
		msgs[i] = m
	}

	return msgs

}

func (ktw *KafkaTestWriter) Write(messages []kafka.Message) (err error) {
	if err := ktw.Writer.WriteMessages(context.Background(), messages...); err != nil {
		log.Printf("could not write message %v", err)
	}
	return
}
