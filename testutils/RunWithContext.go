package testutils

import (
	"context"
	"sync"
	"testing"
	"time"
)

// base function to control the exeecution of schemas handling, merging, aggregation and reading the aggregations topics
func RunWithTimeout(t *testing.T, name string, timeout time.Duration, f func(ctx context.Context), wg *sync.WaitGroup) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	go f(ctx)
	time.Sleep(timeout) // fixme looking ugly, it's just to make sure f(ctx) had enough time
	go func() {
		select {
		case <-ctx.Done():
			t.Logf("context for %s is canceled after %v", name, timeout)
			if wg != nil {
				wg.Done()
			}
			cancel()
		}
	}()
}
