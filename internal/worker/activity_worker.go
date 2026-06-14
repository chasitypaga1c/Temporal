package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"chasitypaga1c/Temporal/internal/activity"
)

var (
	ErrNotFound        = errors.New("activity task not found")
	ErrEntityNotExists = errors.New("entity not exists")
)

type ActivityWorker struct {
	executor *activity.Executor
	mu       sync.Mutex
	respondCompleteFunc func(taskID string, result interface{}) error
}

func NewActivityWorker(respondComplete func(taskID string, result interface{}) error) *ActivityWorker {
	return &ActivityWorker{
		executor:            activity.NewExecutor(),
		respondCompleteFunc: respondComplete,
	}
}

func (w *ActivityWorker) ExecuteActivity(ctx context.Context, taskID string, fn func(ctx context.Context) (interface{}, error), heartbeatTimeout time.Duration) (interface{}, error) {
	resChan, errChan := w.executor.Start(ctx, taskID, fn)

	select {
	case res := <-resChan:
		// Check if context was cancelled before completing
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		
		err := w.respondCompleteFunc(taskID, res)
		if err != nil {
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrEntityNotExists) {
				// Gracefully handle server-side NotFound or EntityNotExists
				return nil, nil
			}
			return nil, err
		}
		return res, nil

	case err := <-errChan:
		return nil, err

	case <-ctx.Done():
		w.executor.Cancel(taskID)
		w.executor.Wait(taskID)
		return nil, ctx.Err()
	}
}
