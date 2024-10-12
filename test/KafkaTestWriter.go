package test

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type KafkaTestWriter struct {
	Writer *kafka.Writer
}

func MakeMessageForTopic(topic string, num int) kafka.Message {
	key := fmt.Sprintf("key-%s-%d", topic, num)
	value := fmt.Sprintf("value-%s-%d", topic, num)
	return kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(value),
	}
}

func (ktw *KafkaTestWriter) MakeMessagesForTopic(topic string, num int) []kafka.Message {
	msgs := make([]kafka.Message, num)

	for i, _ := range msgs {
		m := MakeMessageForTopic(topic, i)
		msgs[i] = m
	}

	return msgs
}

func (ktw *KafkaTestWriter) WriteMessage(message kafka.Message) (err error) {
	if err := ktw.Writer.WriteMessages(context.Background(), message); err != nil {
		log.Printf("could not write message %v", err)
	}
	return
}

func (ktw *KafkaTestWriter) Write(messages []kafka.Message) (err error) {
	if err := ktw.Writer.WriteMessages(context.Background(), messages...); err != nil {
		log.Printf("could not write message %v", err)
	}
	return
}

func (ktw *KafkaTestWriter) Close() error {
	return ktw.Writer.Close()
}
