package main

import (
	"sync"
	"time"
)

type Bucket struct {
	token    float64
	refillTS time.Time
}

type RateLimiter struct {
	buckets  map[string]*Bucket
	mutex    sync.Mutex
	rate     float64
	maxToken float64
}

func NewRateLimiter(rate float64, maxToken float64) *RateLimiter {
	return &RateLimiter{
		buckets:  make(map[string]*Bucket),
		rate:     rate,
		maxToken: maxToken,
	}
}

func (rl *RateLimiter) checkRate(client string) bool {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	bucket, exist := rl.buckets[client]
	if !exist {
		rl.buckets[client] = &Bucket{rl.maxToken, time.Now()}
		return true
	}
	bucket.token = min(rl.maxToken, bucket.token+time.Since(bucket.refillTS).Seconds()*rl.rate)
	bucket.refillTS = time.Now()
	if bucket.token >= 1 {
		bucket.token--
		return true
	}
	return false
}
