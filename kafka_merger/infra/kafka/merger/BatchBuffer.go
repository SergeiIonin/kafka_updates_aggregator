package merger

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"sort"
	"sync"
	"time"
)

type KafkaBatchBuffer struct {
	underlying []kafka.Message
	linger     time.Duration
	bufSize    int
	writer     *kafka.Writer
}

func NewBatchBuffer(linger time.Duration, bufSize int, writer *kafka.Writer) *KafkaBatchBuffer {
	underlying := make([]kafka.Message, 0, bufSize)
	return &KafkaBatchBuffer{underlying, linger, bufSize, writer}
}

func (bb *KafkaBatchBuffer) Run(ctx context.Context, ch <-chan kafka.Message) error {
	mutex := &sync.Mutex{}
	for {
		select {
		case msg := <-ch:
			if err := bb.AddToBatch(ctx, msg); err != nil {
				return err
			}
		case <-time.After(bb.linger):
			mutex.Lock()
			if err := bb.ClearBuffer(ctx); err != nil {
				return err
			}
			mutex.Unlock()
		}
	}
}

func (bb *KafkaBatchBuffer) AddToBatch(ctx context.Context, msg kafka.Message) error {
	if len(bb.underlying) >= bb.bufSize {
		log.Printf("[KafkaBatchBuffer] clearing the buffer because of overload")
		if err := bb.ClearBuffer(ctx); err != nil {
			return err
		}
	}
	log.Printf("[KafkaBatchBuffer] adding kafka msg %s to buffer; number of msgs is %d, capacity of buffer is %d", string(msg.Value), len(bb.underlying), bb.bufSize)
	bb.underlying = append(bb.underlying, msg)
	return nil
}

func (bb *KafkaBatchBuffer) ClearBuffer(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err() // fixme
	default:
		log.Printf("[KafkaBatchBuffer] Clearing the buffer... buffer size is %d", len(bb.underlying))
		sortMessagesByTimestamp(bb.underlying)
		if err := bb.writer.WriteMessages(ctx, bb.underlying...); err != nil { // check that bb.underlying is sorted at this call
			return err // todo panic?
		}
		bb.underlying = make([]kafka.Message, 0, bb.bufSize)
		return nil
	}
}

func sortMessagesByTimestamp(messages []kafka.Message) {
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Time.Before(messages[j].Time)
	})
}
