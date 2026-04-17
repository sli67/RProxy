package main

import (
	"net/http"
	"time"
)

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
			h := checkAlive(b.addr)
			b.SetHealthy(h)
			if h {
				backendHealth.WithLabelValues(b.addr).Set(1)
			} else {
				backendHealth.WithLabelValues(b.addr).Set(0)
			}
		}
		time.Sleep(10 * time.Second)
	}
}
