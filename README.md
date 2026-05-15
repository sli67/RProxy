# Load-balancing reverse proxy
## Usage

### Configuration

Edit `config.yaml`:

```yaml
port: 9000
backends:
  - "http://localhost:8000"
  - "http://localhost:8001"
strategy: "P2C"           # "RR" for Round-Robin or "P2C" for Power of 2 Choices
checkAliveTimeout: 3      # health check timeout in seconds
PrometheusPort: 9090
RLRate: 5                 # tokens added per second per IP
RLMaxToken: 15            # max token bucket size per IP
CheckHealthInterval: 1    
```

### Run

```bash
go run .
```

Then visit `http://localhost:9000` for proxied requests and `http://localhost:9090/metrics` for Prometheus metrics.

## Design Decisions

### Why P2C over Least Connections?

Power of Two Choices avoids the O(n) scan of true least-connections selection by randomly sampling two backends. It achieves near-optimal load distribution with O(1) selection, making it more scalable.

### Circuit Breaker States

- **Closed**: Normal operation. Counts consecutive failures.
- **Open**: After 3 consecutive failures, blocks all traffic for 10 seconds.
- **Half-Open**: After cooldown, allows one request to test recovery. Success → Closed; Failure → Open.

### Token Bucket Rate Limiting

Per-IP buckets are refilled at `RLRate` tokens/second up to `RLMaxToken`. A background goroutine cleans up inactive bucket entries every 5 minutes to prevent memory leaks.

## Benchmark

Tested locally on Windows 11, 4 threads, 100 connections, 30s duration, 2 backends:

| Setup                       | Req/s   | Avg Latency | p99 Latency |
|-----------------------------|---------|-------------|-------------|
| Direct to backend           | 241k    | 0.75ms      | 17ms        |
| Through LB (P2C)            | 59.6k   | 2.11ms      | 26ms        |

*Rate limiter disabled during throughput testing.*

## Project Structure
```
├── main.go            # Entry point, config loading, graceful shutdown
├── loadbalancer.go    # Core LB logic, request handling
├── backend.go         # Backend with reverse proxy and circuit breaker
├── strategy.go        # Round Robin & P2C strategies
├── health.go          # Active health checking
├── ratelimiter.go     # Per-IP token bucket
├── logging.go         # Request ID + structured logging middleware
├── metrics.go         # Prometheus metrics registration
└── config.yaml        # Configuration
```