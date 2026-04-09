package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type Bucket struct {
	token    float64
	refillTS time.Time
}

type LoadBalancer struct {
	proxies    []*httputil.ReverseProxy
	proxyAddrs []string
	health     []bool
	cb         []*CircuitBreaker
	counter    atomic.Uint64
	mutex      sync.RWMutex
	rl         *RateLimiter
}

type RateLimiter struct {
	buckets  map[string]*Bucket
	mutex    sync.Mutex
	rate     float64
	maxToken float64
}

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

func NewLoadBalancer(addrs []string) *LoadBalancer {
	var lb LoadBalancer
	lb.rl = NewRateLimiter(5, 15)
	for i, addr := range addrs {
		i := i
		target, _ := url.Parse(addr)
		tmpProxy := httputil.NewSingleHostReverseProxy(target)
		tmpProxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			lb.mutex.Lock()
			if lb.cb[i].state == HalfOpen {
				lb.cb[i].state = Open
			}
			lb.cb[i].downCount += 1
			if lb.cb[i].downCount >= 3 {
				lb.cb[i].state = Open
				lb.cb[i].openTime = time.Now()
			}
			lb.mutex.Unlock()
		}
		tmpProxy.ModifyResponse = func(res *http.Response) error {
			lb.mutex.Lock()
			if lb.cb[i].state == HalfOpen {
				lb.cb[i].state = Closed
				lb.cb[i].downCount = 0
			}
			lb.mutex.Unlock()
			return nil
		}
		lb.proxies = append(lb.proxies, tmpProxy)
		lb.health = append(lb.health, true)
		lb.cb = append(lb.cb, &CircuitBreaker{state: Closed})
	}
	lb.proxyAddrs = addrs
	return &lb
}

func NewRateLimiter(rate float64, maxToken float64) *RateLimiter {
	var rl RateLimiter
	rl.rate = rate
	rl.maxToken = maxToken
	rl.buckets = make(map[string]*Bucket)
	return &rl
}

func (rl *RateLimiter) checkRate(client string) bool {
	bucket, exist := rl.buckets[client]
	if exist {
		bucket.token = min(rl.maxToken, bucket.token+float64(time.Since(bucket.refillTS).Seconds())*rl.rate)
		bucket.refillTS = time.Now()
		if bucket.token >= 1 {
			bucket.token -= 1
			return true
		} else {
			return false
		}
	} else {
		rl.buckets[client] = &Bucket{rl.maxToken, time.Now()}
		return true
	}
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//check RateLimiting
	hostAddr, _, _ := net.SplitHostPort(r.RemoteAddr)
	lb.rl.mutex.Lock()
	if !lb.rl.checkRate(hostAddr) {
		http.Error(w, "Client Token Limit Exceeded", http.StatusTooManyRequests)
		lb.rl.mutex.Unlock()
		return
	}
	lb.rl.mutex.Unlock()

	size := uint64(len(lb.proxies))
	ind := lb.counter.Add(1) % size
	tries := uint64(0)
	lb.mutex.RLock()
	for tries < size && !lb.health[ind] {
		ind = (ind + 1) % uint64(len(lb.proxies))
		tries += 1
	}
	lb.mutex.RUnlock()
	if tries == size {
		http.Error(w, "No Backend Available", http.StatusServiceUnavailable)
	} else {
		lb.mutex.RLock()
		state := lb.cb[ind].state
		openTime := lb.cb[ind].openTime
		lb.mutex.RUnlock()
		switch state {
		case Closed, HalfOpen:
			lb.proxies[ind].ServeHTTP(w, r)
		case Open:
			if time.Since(openTime).Seconds() >= 10 {
				lb.mutex.Lock()
				lb.cb[ind].state = HalfOpen
				lb.mutex.Unlock()
				lb.proxies[ind].ServeHTTP(w, r)
			} else {
				http.Error(w, "Backend Unavailable", http.StatusServiceUnavailable)
			}
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
	hcache := make([]bool, len(lb.proxyAddrs))
	for {
		for i, addr := range lb.proxyAddrs {
			hcache[i] = checkAlive(addr)
		}
		lb.mutex.Lock()
		copy(lb.health, hcache)
		lb.mutex.Unlock()
		time.Sleep(time.Second * 10)
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
	http.ListenAndServe(":8080", lb)
}
