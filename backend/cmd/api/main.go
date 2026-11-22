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
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/auth"
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

	// 在装配任何客户端之前初始化：Tracer 取的是全局 provider，
	// 先建好才能保证 outbox relay、broker、payment client 都拿到真的 tracer。
	shutdownTracing, err := obs.InitTracing(ctx, "api", cfg.Version)
	if err != nil {
		log.Error("tracing_init_failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		// 用 Background 加超时，而不是已经取消的 ctx：
		// 收到 SIGTERM 时 ctx 已经 done，拿它去 shutdown 会一帧都发不出去。
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(sctx)
	}()

	st, err := store.Open(ctx, cfg.DatabaseURL, log,
		cfg.DBMaxOpen, cfg.DBMaxIdle, cfg.DBConnMaxLifetime)
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
		// 不退出：Redis 只承担商品目录缓存，下单链路一次都不读它。
		// 启动时连不上只意味着「接下来一段时间缓存全穿透」，
		// 从启动就 CrashLoop 反而是把软依赖当成了硬依赖。
		log.Warn("redis_unavailable_at_startup", "err", err, "impact", "缓存穿透回源，/readyz 报 degraded")
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

	// 队列深度和 outbox 积压没有天然的 Prometheus 数据源，
	// 只能主动采样。15s 一次，够告警用，也不会把 broker 打疼。
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if depth, err := brk.QueueDepth(broker.QueueOrderCreated); err == nil {
				obs.QueueDepth.Set(float64(depth))
			}
			sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if n, err := st.CountOutboxPending(sctx); err == nil {
				obs.OutboxPending.Set(float64(n))
			}
			cancel()
		}
	}()

	host, _ := os.Hostname()

	// JWT 验签器是可选的：没配密钥时 api 把所有人当匿名，
	// 本地只起 api+worker 也能完整下单。配了就按 token 归属订单。
	var signer *auth.Signer
	if cfg.JWTSecret != "" {
		signer, err = auth.NewSigner(cfg.JWTSecret, cfg.JWTIssuer, cfg.JWTTTL)
		if err != nil {
			log.Error("jwt_signer_init_failed", "err", err)
			os.Exit(1)
		}
	} else {
		log.Warn("jwt_disabled", "reason", "JWT_SECRET 未设置，所有请求按匿名处理")
	}

	srv := &api.Server{Log: log, Store: st, Cache: c, Broker: brk, Cfg: cfg, Pod: host, Signer: signer}

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
