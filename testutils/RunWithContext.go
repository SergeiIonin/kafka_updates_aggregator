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

func RunWithContext(ctx context.Context, cancel context.CancelFunc, t *testing.T, name string, f func(ctx context.Context)) {
	go f(ctx)
	select {
	case <-ctx.Done():
		t.Logf("context for %s is canceled", name)
		cancel()
	}
}
