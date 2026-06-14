package worker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestActivityWorker_HeartbeatTimeout_LateCompletion(t *testing.T) {
	var mu sync.Mutex
	completedCalled := false
	respondErr := ErrNotFound

	worker := NewActivityWorker(func(taskID string, result interface{}) error {
		mu.Lock()
		completedCalled = true
		mu.Unlock()
		return respondErr
	})

	ctx, cancel := context.WithCancel(context.Background())
	
	activityFn := func(ctx context.Context) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return "success", nil
		}
	}

	errChan := make(chan error, 1)
	go func() {
		_, err := worker.ExecuteActivity(ctx, "task-1", activityFn, 50*time.Millisecond)
		errChan <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	err := <-errChan
	if err == nil {
		t.Fatal("expected error due to cancellation/timeout, got nil")
	}

	mu.Lock()
	called := completedCalled
	mu.Unlock()

	if called {
		t.Error("expected RespondActivityTaskCompleted not to be called or result discarded")
	}
}

func TestActivityWorker_GracefulHandleNotFound(t *testing.T) {
	worker := NewActivityWorker(func(taskID string, result interface{}) error {
		return ErrNotFound
	})

	ctx := context.Background()
	activityFn := func(ctx context.Context) (interface{}, error) {
		return "success", nil
	}

	res, err := worker.ExecuteActivity(ctx, "task-2", activityFn, 1*time.Second)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if res != nil {
		t.Fatalf("expected nil result on NotFound error, got %v", res)
	}
}

func TestActivityWorker_SequentialRetry(t *testing.T) {
	worker := NewActivityWorker(func(taskID string, result interface{}) error {
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	
	running := make(chan struct{})
	released := make(chan struct{})

	activityFn := func(ctx context.Context) (interface{}, error) {
		close(running)
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		close(released)
		return nil, ctx.Err()
	}

	go func() {
		worker.ExecuteActivity(ctx, "task-3", activityFn, 1*time.Second)
	}()

	<-running
	cancel()

	tryStarted := make(chan struct{})
	go func() {
		newCtx := context.Background()
		worker.ExecuteActivity(newCtx, "task-3", func(ctx context.Context) (interface{}, error) {
			close(tryStarted)
			return "retry-success", nil
		}, 1*time.Second)
	}()

	select {
	case <-tryStarted:
		t.Fatal("retry started before first attempt resources were released")
	case <-time.After(20 * time.Millisecond):
		// Good, retry is waiting
	}

	<-released

	select {
	case <-tryStarted:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("retry did not start after resources were released")
	}
}
