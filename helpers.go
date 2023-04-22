package gue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var rnd = rand.New(rand.NewSource(time.Now().Unix()))

// RandomStringID returns random alphanumeric string that can be used as ID.
func RandomStringID() string {
	var data = make([]byte, 32)
	rnd.Read(data)
	hash := sha256.Sum256(data)
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
