package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/broker"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/cache"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/store"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/worker"
)

func main() {
	cfg, err := config.Load("worker")
	if err != nil {
		fmt.Fprintln(os.Stderr, "配置错误:", err)
		os.Exit(1)
	}

	log := obs.NewLogger(cfg.LogLevel, "worker", cfg.Version)
	if host, err := os.Hostname(); err == nil {
		log = log.With("pod", host)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("database_open_failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// worker 不跑迁移：schema 归 api 管，两个进程同时迁移只会互相等锁。
	c, err := cache.New(ctx, cfg.RedisAddr, cfg.RedisPass, cfg.CacheTTL)
	if err != nil {
		log.Error("redis_init_failed", "err", err)
		os.Exit(1)
	}
	defer c.Close()

	brk, err := broker.Connect(ctx, cfg.RabbitURL, log)
	if err != nil {
		log.Error("rabbitmq_connect_failed", "err", err)
		os.Exit(1)
	}
	defer brk.Close()
	if err := brk.Declare(); err != nil {
		log.Error("topology_declare_failed", "err", err)
		os.Exit(1)
	}

	// worker 也需要 HTTP：探针和 /metrics 都靠它
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","role":"worker"}`))
	})
	mux.Handle("GET /metrics", obs.Handler())

	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http_server_failed", "err", err)
			os.Exit(1)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	w := &worker.Worker{Log: log, Store: st, Cache: c, Broker: brk, Cfg: cfg}
	log.Info("worker_starting", "queue", broker.QueueOrderCreated, "prefetch", cfg.Prefetch, "max_attempts", cfg.MaxAttempts)
	if err := w.Run(ctx); err != nil {
		log.Error("worker_stopped_with_error", "err", err)
		os.Exit(1)
	}
	log.Info("worker_stopped")
}
