package main

import (
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type ProxyState int

const (
	Closed ProxyState = iota
	Open
	HalfOpen
)

type CircuitBreaker struct {
	state     ProxyState
	downCount int
	openTime  time.Time
}

type Backend struct {
	addr     string
	proxy    *httputil.ReverseProxy
	healthy  atomic.Bool
	mu       sync.RWMutex // guards cb
	cb       CircuitBreaker
	inflight atomic.Int64
}

func (b *Backend) IsHealthy() bool { return b.healthy.Load() }

func (b *Backend) SetHealthy(v bool) { b.healthy.Store(v) }

func (b *Backend) CBState() (ProxyState, time.Time) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.cb.state, b.cb.openTime
}

func (b *Backend) onError() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cb.state == HalfOpen {
		b.cb.state = Open
		b.cb.openTime = time.Now()
		return
	}
	b.cb.downCount++
	if b.cb.downCount >= 3 {
		b.cb.state = Open
		b.cb.openTime = time.Now()
	}
}

func (b *Backend) onSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cb.state == HalfOpen {
		b.cb.state = Closed
		b.cb.downCount = 0
	}
}

func (b *Backend) tryHalfOpen() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cb.state = HalfOpen
}

func NewBackend(addr string) *Backend {
	target, _ := url.Parse(addr)
	b := &Backend{
		addr:  addr,
		proxy: httputil.NewSingleHostReverseProxy(target),
		cb:    CircuitBreaker{state: Closed},
	}
	b.healthy.Store(true)
	b.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		b.onError()
	}
	b.proxy.ModifyResponse = func(res *http.Response) error {
		b.onSuccess()
		return nil
	}
	return b
}

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

type LoadBalancer struct {
	backends []*Backend
	rl       *RateLimiter
	strategy Strategy
}

func NewLoadBalancer(addrs []string) *LoadBalancer {
	lb := &LoadBalancer{rl: NewRateLimiter(5, 15)}
	for _, addr := range addrs {
		lb.backends = append(lb.backends, NewBackend(addr))
	}
	lb.strategy = &P2C{}
	return lb
}

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

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hostAddr, _, _ := net.SplitHostPort(r.RemoteAddr)
	if !lb.rl.checkRate(hostAddr) {
		http.Error(w, "Client Token Limit Exceeded", http.StatusTooManyRequests)
		return
	}

	b, ok := lb.strategy.Pick(lb.backends)
	if !ok {
		http.Error(w, "No Backend Available", http.StatusServiceUnavailable)
		return
	}

	state, openTime := b.CBState()
	switch state {
	case Closed, HalfOpen:
		b.inflight.Add(1)
		defer b.inflight.Add(-1)
		b.proxy.ServeHTTP(w, r)
	case Open:
		if time.Since(openTime).Seconds() >= 10 {
			b.tryHalfOpen()
			b.inflight.Add(1)
			defer b.inflight.Add(-1)
			b.proxy.ServeHTTP(w, r)
		} else {
			http.Error(w, "Backend Unavailable", http.StatusServiceUnavailable)
		}
	}
}

var cl = http.Client{Timeout: 3 * time.Second}

func checkAlive(u string) bool {
	res, err := cl.Get(u)
	if err == nil && res.StatusCode == 200 {
		res.Body.Close()
		return true
	}
	return false
}

func (lb *LoadBalancer) checkHealth() {
	for {
		for _, b := range lb.backends {
			b.SetHealthy(checkAlive(b.addr))
		}
		time.Sleep(10 * time.Second)
	}
}

var backHosts = []string{
	"http://localhost:8000",
	"http://localhost:8001",
}

func main() {
	lb := NewLoadBalancer(backHosts)
	go lb.checkHealth()
	fmt.Println("Reverse proxy starting on :8080")
	http.ListenAndServe("0.0.0.0:8080", lb)
}
