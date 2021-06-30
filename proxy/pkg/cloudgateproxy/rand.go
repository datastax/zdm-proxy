package cloudgateproxy

import (
	"math/rand"
	"sync"
	"time"
)

func NewThreadSafeRand() *rand.Rand {
	return rand.New(&lockedSource{
		lk:  sync.Mutex{},
		src: rand.NewSource(time.Now().Unix()),
	})
}

type lockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func (r *lockedSource) Int63() (n int64) {
	r.lk.Lock()
	n = r.src.Int63()
	r.lk.Unlock()
	return
}

func (r *lockedSource) Seed(seed int64) {
	r.lk.Lock()
	r.src.Seed(seed)
	r.lk.Unlock()
}