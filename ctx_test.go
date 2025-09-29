package gue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetWorkerIdx(t *testing.T) {
	t.Run("no ctx", func(t *testing.T) {
		idx := GetWorkerIdx(context.TODO())
		assert.Equal(t, WorkerIdxUnknown, idx)
	})

	t.Run("no idx in the ctx", func(t *testing.T) {
		ctx := context.Background()
		idx := GetWorkerIdx(ctx)
		assert.Equal(t, WorkerIdxUnknown, idx)
	})

	t.Run("idx is set", func(t *testing.T) {
		ctx := setWorkerIdx(context.Background(), 99)
		idx := GetWorkerIdx(ctx)
		assert.Equal(t, 99, idx)
	})
}
