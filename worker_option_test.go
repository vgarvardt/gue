package gue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWakeInterval(t *testing.T) {
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerWithDefaultInterval := NewWorker(nil, wm)
	assert.Equal(t, defaultWakeInterval, workerWithDefaultInterval.interval)

	customInterval := 12345 * time.Millisecond
	workerWithCustomInterval := NewWorker(nil, wm, WakeInterval(customInterval))
	assert.Equal(t, customInterval, workerWithCustomInterval.interval)
}

func TestWorkerQueue(t *testing.T) {
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerWithDefaultQueue := NewWorker(nil, wm)
	assert.Equal(t, defaultQueueName, workerWithDefaultQueue.queue)

	customQueue := "fooBarBaz"
	workerWithCustomQueue := NewWorker(nil, wm, WorkerQueue(customQueue))
	assert.Equal(t, customQueue, workerWithCustomQueue.queue)
}

func TestWorkerID(t *testing.T) {
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerWithDefaultID := NewWorker(nil, wm)
	assert.NotEmpty(t, workerWithDefaultID.id)

	customID := "some-meaningful-id"
	workerWithCustomID := NewWorker(nil, wm, WorkerID(customID))
	assert.Equal(t, customID, workerWithCustomID.id)
}

func TestPoolWakeInterval(t *testing.T) {
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerPoolWithDefaultInterval := NewWorkerPool(nil, wm, 2)
	assert.Equal(t, defaultWakeInterval, workerPoolWithDefaultInterval.interval)

	customInterval := 12345 * time.Millisecond
	workerPoolWithCustomInterval := NewWorkerPool(nil, wm, 2, PoolWakeInterval(customInterval))
	assert.Equal(t, customInterval, workerPoolWithCustomInterval.interval)
}

func TestPoolWorkerQueue(t *testing.T) {
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerPoolWithDefaultQueue := NewWorkerPool(nil, wm, 2)
	assert.Equal(t, defaultQueueName, workerPoolWithDefaultQueue.queue)

	customQueue := "fooBarBaz"
	workerPoolWithCustomQueue := NewWorkerPool(nil, wm, 2, PoolWorkerQueue(customQueue))
	assert.Equal(t, customQueue, workerPoolWithCustomQueue.queue)
}

func TestPoolWorkerID(t *testing.T) {
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerPoolWithDefaultID := NewWorkerPool(nil, wm, 2)
	assert.NotEmpty(t, workerPoolWithDefaultID.id)

	customID := "some-meaningful-id"
	workerPoolWithCustomID := NewWorkerPool(nil, wm, 2, PoolWorkerID(customID))
	assert.Equal(t, customID, workerPoolWithCustomID.id)
}
