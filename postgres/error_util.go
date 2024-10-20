package postgres

import (
	"context"
	"errors"
	"github.com/lib/pq"
)

func IsCancellationError(err error) bool {
	if err == nil {
		return false
	}

	pgErr, ok := err.(*pq.Error)
	if ok {
		if pgErr.Code == "57014" {
			return true
		}
	}

	return errors.Is(err, context.Canceled)
}
