package domain

import (
	"context"
	"errors"
)

func ContextCanceledOrDeadlineExceeded(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
