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

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/api"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/broker"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/cache"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/httpx"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/outbox"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/store"
)

func main() {
	cfg, err := config.Load("api")
	if err != nil {
		fmt.Fprintln(os.Stderr, "配置错误:", err)
		os.Exit(1)
	}

	log := obs.NewLogger(cfg.LogLevel, "api", cfg.Version)
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

	// 迁移必须同步跑完再对外服务：失败就 CrashLoop，让 K8s 重试，
	// 而不是带着缺表的结构对外返回 500。
	applied, err := st.Migrate(ctx)
	if err != nil {
		log.Error("migration_failed", "err", err)
		os.Exit(1)
	}
	log.Info("migrations_applied", "count", len(applied), "versions", applied)

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

	go outbox.Run(ctx, log, st, brk, 2*time.Second)

	host, _ := os.Hostname()
	srv := &api.Server{Log: log, Store: st, Cache: c, Broker: brk, Cfg: cfg, Pod: host}

	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           httpx.Wrap(log, srv.Handler()),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      20 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		log.Info("shutdown_started")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	log.Info("api_listening", "addr", cfg.Addr, "cache_ttl", cfg.CacheTTL.String(), "dev_mode", cfg.DevMode)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http_server_failed", "err", err)
		os.Exit(1)
	}
	log.Info("api_stopped")
}
