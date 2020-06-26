package gue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vgarvardt/gue/v2/adapter"
)

func TestWithClientID(t *testing.T) {
	clientWithDefaultID := NewClient(nil)
	assert.NotEmpty(t, clientWithDefaultID.id)

	customID := "some-meaningful-id"
	clientWithCustomID := NewClient(nil, WithClientID(customID))
	assert.Equal(t, customID, clientWithCustomID.id)
}

func TestWithClientLogger(t *testing.T) {
	clientWithDefaultLogger := NewClient(nil)
	assert.IsType(t, adapter.NoOpLogger{}, clientWithDefaultLogger.logger)

	logMessage := "hello"

	l := new(mockLogger)
	l.On("Info", logMessage, mock.Anything)
	// worker sets id as default logger field
	l.On("With", mock.Anything).Return(l)

	clientWithCustomLogger := NewClient(nil, WithClientLogger(l))
	clientWithCustomLogger.logger.Info(logMessage)

	l.AssertExpectations(t)
}
