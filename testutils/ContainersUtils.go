package testutils

import (
	"context"
	tc "github.com/testcontainers/testcontainers-go"
	"testing"
)

func TerminateContainer(container tc.Container, ctx context.Context, t *testing.T) {
	err := container.Terminate(ctx)
	if err != nil {
		t.Fatalf(err.Error())
	}
}
