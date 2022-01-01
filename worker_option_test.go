package gue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vgarvardt/gue/v3/adapter"
)

type mockLogger struct {
	mock.Mock
}

func (m *mockLogger) Debug(msg string, fields ...adapter.Field) {
	m.Called(msg, fields)
}

func (m *mockLogger) Info(msg string, fields ...adapter.Field) {
	m.Called(msg, fields)
}

func (m *mockLogger) Error(msg string, fields ...adapter.Field) {
	m.Called(msg, fields)
}

func (m *mockLogger) With(fields ...adapter.Field) adapter.Logger {
	args := m.Called(fields)
	return args.Get(0).(adapter.Logger)
}

var dummyWM = WorkMap{
	"MyJob": func(ctx context.Context, j *Job) error {
		return nil
	},
}

func TestWithWorkerPollInterval(t *testing.T) {
	workerWithDefaultInterval := NewWorker(nil, dummyWM)
	assert.Equal(t, defaultPollInterval, workerWithDefaultInterval.interval)

	customInterval := 12345 * time.Millisecond
	workerWithCustomInterval := NewWorker(nil, dummyWM, WithWorkerPollInterval(customInterval))
	assert.Equal(t, customInterval, workerWithCustomInterval.interval)
}

func TestWithWorkerQueue(t *testing.T) {
	workerWithDefaultQueue := NewWorker(nil, dummyWM)
	assert.Equal(t, defaultQueueName, workerWithDefaultQueue.queue)

	customQueue := "fooBarBaz"
	workerWithCustomQueue := NewWorker(nil, dummyWM, WithWorkerQueue(customQueue))
	assert.Equal(t, customQueue, workerWithCustomQueue.queue)
}

func TestWithWorkerID(t *testing.T) {
	workerWithDefaultID := NewWorker(nil, dummyWM)
	assert.NotEmpty(t, workerWithDefaultID.id)

	customID := "some-meaningful-id"
	workerWithCustomID := NewWorker(nil, dummyWM, WithWorkerID(customID))
	assert.Equal(t, customID, workerWithCustomID.id)
}

func TestWithWorkerLogger(t *testing.T) {
	workerWithDefaultLogger := NewWorker(nil, dummyWM)
	assert.IsType(t, adapter.NoOpLogger{}, workerWithDefaultLogger.logger)

	logMessage := "hello"

	l := new(mockLogger)
	l.On("Info", logMessage, mock.Anything)
	// worker sets id as default logger field
	l.On("With", mock.Anything).Return(l)

	workerWithCustomLogger := NewWorker(nil, dummyWM, WithWorkerLogger(l))
	workerWithCustomLogger.logger.Info(logMessage)

	l.AssertExpectations(t)
}

func TestWithWorkerPollStrategy(t *testing.T) {
	workerWithWorkerPollStrategy := NewWorker(nil, dummyWM, WithWorkerPollStrategy(RunAtPollStrategy))
	assert.Equal(t, RunAtPollStrategy, workerWithWorkerPollStrategy.pollStrategy)
}

func TestWithPoolPollInterval(t *testing.T) {
	workerPoolWithDefaultInterval := NewWorkerPool(nil, dummyWM, 2)
	assert.Equal(t, defaultPollInterval, workerPoolWithDefaultInterval.interval)

	customInterval := 12345 * time.Millisecond
	workerPoolWithCustomInterval := NewWorkerPool(nil, dummyWM, 2, WithPoolPollInterval(customInterval))
	assert.Equal(t, customInterval, workerPoolWithCustomInterval.interval)
}

func TestWithPoolQueue(t *testing.T) {
	workerPoolWithDefaultQueue := NewWorkerPool(nil, dummyWM, 2)
	assert.Equal(t, defaultQueueName, workerPoolWithDefaultQueue.queue)

	customQueue := "fooBarBaz"
	workerPoolWithCustomQueue := NewWorkerPool(nil, dummyWM, 2, WithPoolQueue(customQueue))
	assert.Equal(t, customQueue, workerPoolWithCustomQueue.queue)
}

func TestWithPoolID(t *testing.T) {
	workerPoolWithDefaultID := NewWorkerPool(nil, dummyWM, 2)
	assert.NotEmpty(t, workerPoolWithDefaultID.id)

	customID := "some-meaningful-id"
	workerPoolWithCustomID := NewWorkerPool(nil, dummyWM, 2, WithPoolID(customID))
	assert.Equal(t, customID, workerPoolWithCustomID.id)
}

func TestWithPoolLogger(t *testing.T) {
	workerPoolWithDefaultLogger := NewWorkerPool(nil, dummyWM, 2)
	assert.IsType(t, adapter.NoOpLogger{}, workerPoolWithDefaultLogger.logger)

	logMessage := "hello"

	l := new(mockLogger)
	l.On("Info", logMessage, mock.Anything)
	// worker pool sets id as default logger field
	l.On("With", mock.Anything).Return(l)

	workerPoolWithCustomLogger := NewWorkerPool(nil, dummyWM, 2, WithPoolLogger(l))
	workerPoolWithCustomLogger.logger.Info(logMessage)

	l.AssertExpectations(t)
}

func TestWithPoolPollStrategy(t *testing.T) {
	workerPoolWithPoolPollStrategy := NewWorkerPool(nil, dummyWM, 2, WithPoolPollStrategy(RunAtPollStrategy))
	assert.Equal(t, RunAtPollStrategy, workerPoolWithPoolPollStrategy.pollStrategy)
}
