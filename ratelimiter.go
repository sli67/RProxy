package main

import (
	"sync"
	"time"
)

const (
	numShards = 256
	shardMask = numShards - 1
)

type Bucket struct {
	token    float64
	refillTS time.Time
}

type shard struct {
	mu      sync.Mutex
	buckets map[string]*Bucket
}

type RateLimiter struct {
	shards   [numShards]shard
	rate     float64
	maxToken float64
}

func NewRateLimiter(rate float64, maxToken float64) *RateLimiter {
	rl := &RateLimiter{
		rate:     rate,
		maxToken: maxToken,
	}
	for i := range rl.shards {
		rl.shards[i].buckets = make(map[string]*Bucket)
	}
	go rl.cleanup()
	return rl
}

func shardIndex(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h & shardMask
}

func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		for i := range rl.shards {
			s := &rl.shards[i]
			s.mu.Lock()
			for ip, bucket := range s.buckets {
				if time.Since(bucket.refillTS) > 5*time.Minute {
					delete(s.buckets, ip)
				}
			}
			s.mu.Unlock()
		}
	}
}

func (rl *RateLimiter) checkRate(client string) bool {
	s := &rl.shards[shardIndex(client)]
	s.mu.Lock()
	defer s.mu.Unlock()
	bucket, exist := s.buckets[client]
	if !exist {
		s.buckets[client] = &Bucket{rl.maxToken, time.Now()}
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
