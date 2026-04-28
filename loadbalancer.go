package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type LoadBalancer struct {
	backends          []*Backend
	rl                *RateLimiter
	strategy          Strategy
	checkAliveTimeout time.Duration
}

func NewLoadBalancer(config Config) *LoadBalancer {
	timeout := time.Duration(config.CheckAliveTimeout) * time.Second
	if timeout == 0 {
		timeout = 3 * time.Second
	}
	rlRate := config.RLRate
	if rlRate == 0 {
		rlRate = 5
	}
	rlMax := float64(config.RLMaxToken)
	if rlMax == 0 {
		rlMax = 15
	}
	lb := &LoadBalancer{rl: NewRateLimiter(rlRate, rlMax), checkAliveTimeout: timeout}
	for _, addr := range config.Backends {
		b, err := NewBackend(addr)
		if err != nil {
			slog.Error("invalid backend address", "addr", addr, "err", err)
			continue
		}
		lb.backends = append(lb.backends, b)
	}
	switch config.Strategy {
	case "RR":
		lb.strategy = &RoundRobin{}
	case "P2C":
		lb.strategy = &P2C{}
	default:
		lb.strategy = &RoundRobin{}
	}
	return lb
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.Header.Get("X-Request-ID")
	if id == "" {
		id = uuid.NewString()
	}
	w.Header().Set("X-Request-ID", id)
	r = r.WithContext(context.WithValue(r.Context(), requestIDKey, id))
	logw := &loggingRW{ResponseWriter: w}

	hostAddr, _, _ := net.SplitHostPort(r.RemoteAddr)

	//defer logging
	start := time.Now()
	var backendAddr string
	defer func() {
		slog.Info("request",
			"request_id", id,
			"method", r.Method,
			"path", r.URL.Path,
			"remote", hostAddr,
			"backend", backendAddr,
			"status", logw.status,
			"bytes", logw.bytes,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}()

	if !lb.rl.checkRate(hostAddr) {
		http.Error(logw, "Client Token Limit Exceeded", http.StatusTooManyRequests)
		return
	}

	b, ok := lb.strategy.Pick(lb.backends)
	if !ok {
		http.Error(logw, "No Backend Available", http.StatusServiceUnavailable)
		return
	}
	backendAddr = b.addr

	state, openTime := b.CBState()
	sTime := time.Now()
	switch state {
	case Closed, HalfOpen:
		b.inflight.Add(1)
		activeConnections.WithLabelValues(b.addr).Inc()
		defer activeConnections.WithLabelValues(b.addr).Dec()
		defer b.inflight.Add(-1)

		b.proxy.ServeHTTP(logw, r)
		requestDuration.WithLabelValues(b.addr).Observe(time.Since(sTime).Seconds())
	case Open:
		if time.Since(openTime).Seconds() >= 10 {
			b.tryHalfOpen()

			b.inflight.Add(1)
			activeConnections.WithLabelValues(b.addr).Inc()
			defer activeConnections.WithLabelValues(b.addr).Dec()
			defer b.inflight.Add(-1)

			b.proxy.ServeHTTP(logw, r)
			requestDuration.WithLabelValues(b.addr).Observe(time.Since(sTime).Seconds())
		} else {
			http.Error(logw, "Backend Unavailable", http.StatusServiceUnavailable)
		}
	}
}
