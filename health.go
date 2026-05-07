package main

import (
	"net/http"
	"time"
)

func (lb *LoadBalancer) checkHealth(checkHealthInterval int) {
	cl := http.Client{Timeout: lb.checkAliveTimeout}
	for {
		for _, b := range lb.backends {
			res, err := cl.Get(b.addr)
			alive := err == nil && res.StatusCode == 200
			b.SetHealthy(alive)
			if alive {
				backendHealth.WithLabelValues(b.addr).Set(1)
				state, _ := b.CBState()
				if state == Open {
					b.tryHalfOpen()
				}
				b.onSuccess(res.StatusCode)
			} else {
				backendHealth.WithLabelValues(b.addr).Set(0)
				b.onError()
			}
			if alive {
				res.Body.Close()
			}
		}
		time.Sleep(time.Duration(checkHealthInterval) * time.Second)
	}
}
