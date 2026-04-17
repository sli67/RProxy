package main

import (
	"math/rand/v2"
	"sync/atomic"
)

type Strategy interface {
	Pick(backends []*Backend) (*Backend, bool)
}

type RoundRobin struct {
	counter atomic.Uint64
}

type P2C struct {
}

func (rr *RoundRobin) Pick(backends []*Backend) (*Backend, bool) {
	size := uint64(len(backends))
	start := rr.counter.Add(1) % size
	for i := uint64(0); i < size; i++ {
		ind := (start + i) % size
		if backends[ind].IsHealthy() {
			return backends[ind], true
		}
	}
	return nil, false
}

var _ Strategy = (*RoundRobin)(nil)

func (p *P2C) Pick(backends []*Backend) (*Backend, bool) {
	size := uint64(len(backends))
	healthyPointers := make([]*Backend, 0, size)
	for i := uint64(0); i < size; i++ {
		if backends[i].IsHealthy() {
			healthyPointers = append(healthyPointers, backends[i])
		}
	}
	if len(healthyPointers) == 0 {
		return nil, false
	}
	if len(healthyPointers) == 1 {
		return healthyPointers[0], true
	}
	i := rand.IntN(len(healthyPointers))
	j := rand.IntN(len(healthyPointers) - 1)
	if j >= i {
		j += 1
	}
	if healthyPointers[i].inflight.Load() <= healthyPointers[j].inflight.Load() {
		return healthyPointers[i], true
	}
	return healthyPointers[j], true
}

var _ Strategy = (*P2C)(nil)
