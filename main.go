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

func NewLoadBalancer(addrs []string) *LoadBalancer {
	var lb LoadBalancer
	lb.rl = NewRateLimiter(5, 15)
	for _, addr := range addrs {
		target, _ := url.Parse(addr)
		lb.proxies = append(lb.proxies, httputil.NewSingleHostReverseProxy(target))
		lb.health = append(lb.health, true)
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
		lb.proxies[ind].ServeHTTP(w, r)
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
