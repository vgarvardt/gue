package gue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWakeInterval(t *testing.T) {
	c := openTestClient(t)

	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerWithDefaultInterval := NewWorker(c, wm)
	assert.Equal(t, defaultWakeInterval, workerWithDefaultInterval.Interval)

	customInterval := 12345 * time.Millisecond
	workerWithCustomInterval := NewWorker(c, wm, WakeInterval(customInterval))
	assert.Equal(t, customInterval, workerWithCustomInterval.Interval)
}

func TestWorkerQueue(t *testing.T) {
	c := openTestClient(t)

	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerWithDefaultQueue := NewWorker(c, wm)
	assert.Equal(t, defaultQueueName, workerWithDefaultQueue.Queue)

	customQueue := "fooBarBaz"
	workerWithCustomQueue := NewWorker(c, wm, WorkerQueue(customQueue))
	assert.Equal(t, customQueue, workerWithCustomQueue.Queue)
}

func TestPoolWakeInterval(t *testing.T) {
	c := openTestClient(t)

	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerPoolWithDefaultInterval := NewWorkerPool(c, wm, 2)
	assert.Equal(t, defaultWakeInterval, workerPoolWithDefaultInterval.Interval)

	customInterval := 12345 * time.Millisecond
	workerPoolWithCustomInterval := NewWorkerPool(c, wm, 2, PoolWakeInterval(customInterval))
	assert.Equal(t, customInterval, workerPoolWithCustomInterval.Interval)
}

func TestPoolWorkerQueue(t *testing.T) {
	c := openTestClient(t)

	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerPoolWithDefaultQueue := NewWorkerPool(c, wm, 2)
	assert.Equal(t, defaultQueueName, workerPoolWithDefaultQueue.Queue)

	customQueue := "fooBarBaz"
	workerPoolWithCustomQueue := NewWorkerPool(c, wm, 2, PoolWorkerQueue(customQueue))
	assert.Equal(t, customQueue, workerPoolWithCustomQueue.Queue)
}
