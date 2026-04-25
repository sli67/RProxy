package main

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
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

func (b *Backend) onSuccess(_ int) {
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

func NewBackend(addr string) (*Backend, error) {
	target, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
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
		requestCount.WithLabelValues(b.addr, strconv.Itoa(res.StatusCode)).Inc()
		if res.StatusCode >= 500 {
			b.onError()
		} else {
			b.onSuccess(res.StatusCode)
		}
		return nil
	}
	return b, nil
}
