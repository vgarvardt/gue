package gue

import (
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
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
