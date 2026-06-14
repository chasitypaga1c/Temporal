package activity

import (
	"context"
	"sync"
)

type Executor struct {
	mu             sync.Mutex
	activeTasks    map[string]context.CancelFunc
	taskWaitGroups map[string]*sync.WaitGroup
}

func NewExecutor() *Executor {
	return &Executor{
		activeTasks:    make(map[string]context.CancelFunc),
		taskWaitGroups: make(map[string]*sync.WaitGroup),
	}
}

func (e *Executor) Start(ctx context.Context, taskID string, fn func(ctx context.Context) (interface{}, error)) (<-chan interface{}, <-chan error) {
	e.mu.Lock()
	
	// Ensure previous execution is fully released before starting a new one
	if wg, exists := e.taskWaitGroups[taskID]; exists {
		e.mu.Unlock()
		wg.Wait()
		e.mu.Lock()
	}

	taskCtx, cancel := context.WithCancel(ctx)
	e.activeTasks[taskID] = cancel

	wg := &sync.WaitGroup{}
	wg.Add(1)
	e.taskWaitGroups[taskID] = wg
	e.mu.Unlock()

	resChan := make(chan interface{}, 1)
	errChan := make(chan error, 1)

	go func() {
		defer wg.Done()
		defer func() {
		e.mu.Lock()
		delete(e.activeTasks, taskID)
		e.mu.Unlock()
		}()

		res, err := fn(taskCtx)
		if err != nil {
			errChan <- err
			return
		}
		resChan <- res
	}()

	return resChan, errChan
}

func (e *Executor) Cancel(taskID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if cancel, exists := e.activeTasks[taskID]; exists {
		cancel()
	}
}

func (e *Executor) Wait(taskID string) {
	e.mu.Lock()
	wg, exists := e.taskWaitGroups[taskID]
	e.mu.Unlock()
	if exists {
		wg.Wait()
	}
}
