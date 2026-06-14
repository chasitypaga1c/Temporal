package main

import (
	"context"
	"fmt"
	"time"

	"chasitypaga1c/Temporal/internal/worker"
)

func main() {
	fmt.Println("Starting Temporal Activity Worker Demo...")

	w := worker.NewActivityWorker(func(taskID string, result interface{}) error {
		fmt.Printf("Server: RespondActivityTaskCompleted called for %s with result %v\n", taskID, result)
		return worker.ErrNotFound
	})

	ctx, cancel := context.WithCancel(context.Background())

	activityFn := func(ctx context.Context) (interface{}, error) {
		fmt.Println("Activity: Started execution")
		select {
		case <-ctx.Done():
			fmt.Println("Activity: Context cancelled (heartbeat timeout)")
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
			fmt.Println("Activity: Completed execution locally")
			return "success-data", nil
		}
	}

	go func() {
		time.Sleep(1 * time.Second)
		fmt.Println("Worker: Simulating Heartbeat Timeout from server...")
		cancel()
	}()

	res, err := w.ExecuteActivity(ctx, "demo-task", activityFn, 1*time.Second)
	fmt.Printf("Worker: ExecuteActivity returned result: %v, error: %v\n", res, err)
}
