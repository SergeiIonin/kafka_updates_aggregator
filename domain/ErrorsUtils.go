package domain

import (
	"context"
	"errors"
)

func ContextOrDeadlineExceeded(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
