package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Port              int      `yaml:"port"`
	Backends          []string `yaml:"backends"`
	Strategy          string   `yaml:"strategy"`
	CheckAliveTimeout int      `yaml:"checkAliveTimeout"`
}

func main() {
	go http.ListenAndServe("0.0.0.0:9090", promhttp.Handler())
	configFile, err := os.ReadFile("config.yaml")
	if err != nil {
		slog.Error("Could not read config file: ", "error", err)
		return
	}
	var config Config
	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		slog.Error("Could not load config file: ", "error", err)
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	lb := NewLoadBalancer(config)
	myServer := &http.Server{Addr: "0.0.0.0:" + strconv.Itoa(config.Port), Handler: lb}
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("Reverse proxy starting on :" + strconv.Itoa(config.Port))
		go lb.checkHealth()
		if err := myServer.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("Server Error: ", "error", err)
		}
	}()

	<-quit
	slog.Info("Shutting down reverse proxy ...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := myServer.Shutdown(ctx); err != nil {
		slog.Info("Forced shutdown:", "error", err)
	}
	slog.Info("Server stopped")
}
