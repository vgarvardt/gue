package gue

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// RandomStringID returns random alphanumeric string that can be used as ID.
func RandomStringID() string {
	hash := md5.Sum([]byte(time.Now().Format(time.RFC3339Nano)))
	return hex.EncodeToString(hash[:])[:6]
}

// RunLock ensures that there is only one instance of the running callback function "f" (worker).
func RunLock(ctx context.Context, f func(ctx context.Context) error, mu *sync.Mutex, running *bool, id string) error {
	mu.Lock()
	if *running {
		mu.Unlock()
		return fmt.Errorf("worker[id=%s] is already running", id)
	}
	*running = true
	mu.Unlock()

	defer func() {
		mu.Lock()
		*running = false
		mu.Unlock()
	}()

	return f(ctx)
}
