package main

import "github.com/prometheus/client_golang/prometheus"

var requestCount = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "proxy_requests_total",
		Help: "Total number of proxied requests",
	},
	[]string{"backend", "status_code"},
)

var requestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "proxy_request_duration_seconds",
		Help: "Duration of proxied requests in seconds",
	},
	[]string{"backend"},
)

var activeConnections = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "proxy_active_connections",
		Help: "Number of active connections",
	},
	[]string{"backend"},
)

var backendHealth = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "proxy_backend_health",
		Help: "Health status of backend",
	},
	[]string{"backend"},
)

func init() {
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(activeConnections)
	prometheus.MustRegister(backendHealth)
}
