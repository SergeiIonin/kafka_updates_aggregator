package reader

import (
	"context"
	"github.com/segmentio/kafka-go"
	"testing"
)

func Test_Reader_test(t *testing.T) {
	newReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		GroupID:     "test-reader-group-0",
		Topic:       "test_merged",
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.FirstOffset,
	})

	for {
		msg, err := newReader.ReadMessage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("msg key/value/offset/ = %s/%s/%d", string(msg.Key), string(msg.Value), msg.Offset)
	}
}
