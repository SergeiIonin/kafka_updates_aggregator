package kafka_merger

import (
	"container/heap"
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
	"time"
)

type ChannelsMerger struct {
}

func (m *ChannelsMerger) Merge(ctx context.Context, output chan<- kafka.Message, inputs []chan kafka.Message) error {
	var wg sync.WaitGroup
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	mutex := &sync.Mutex{}

	wg.Add(len(inputs))
	for i, ch := range inputs {
		go func(ch <-chan kafka.Message) {
			defer func() {
				log.Printf("[ChannelsMerger] Reading from channel %d is finished", i) // fixme rm
				wg.Done()
			}()
			log.Printf("[ChannelsMerger] Start reading from channel %d", i) // fixme rm
			for msg := range ch {
				mutex.Lock()
				heap.Push(&pq, msg)
				mutex.Unlock()
				log.Printf("[ChannelsMerger] size of queue = %d", len(pq)) // fixme rm
			}
		}(ch)
	}

	// fixme is it ok to move it to defer?
	go func() {
		wg.Wait()
		log.Println("[ChannelsMerger] Closing the output channel") // fixme rm
		close(output)
	}()

	timerDuration := 15 * time.Millisecond // in theory this value is capped by the ReadBackoffMax of consumer (reader), if it's much smaller than that, we may waste some cycles
    timer := time.NewTimer(timerDuration)

	for {
		if len(pq) == 0 {
			continue
		}
		timer.Reset(timerDuration)
		select {
		case <-ctx.Done(): 
			log.Printf("[ChannelsMerger] Merging is canceled") // fixme rm
			return context.Canceled
		case <-timer.C: // fixme use timer and reset it instead, otherwise is not memory-efficient
			log.Printf("[ChannelsMerger] Flushing to output, len(pq) = %d", len(pq)) // fixme rm
			mutex.Lock()
			for len(pq) > 0 {
				output <- heap.Pop(&pq).(kafka.Message)
			}
			mutex.Unlock()
			log.Printf("[ChannelsMerger] Messages flushed") // fixme rm
		}
	}
}

type PriorityQueue []kafka.Message

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Time.Before(pq[j].Time)
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}
func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(kafka.Message)
	*pq = append(*pq, item)
}
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
