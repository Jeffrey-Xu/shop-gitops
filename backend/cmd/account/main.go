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

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/account"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/auth"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/httpx"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/store"
)

// 账号服务独立进程，和 api 共用同一个镜像（靠 command 区分）。
// 分离的理由：注册/登录的流量特征和商品浏览完全不同（一个吃 CPU 做 bcrypt，
// 一个是缓存读），放一起会互相影响；而且账号服务挂了不该影响下单。
func main() {
	cfg, err := config.Load("account")
	if err != nil {
		fmt.Fprintln(os.Stderr, "配置错误:", err)
		os.Exit(1)
	}

	log := obs.NewLogger(cfg.LogLevel, "account", cfg.Version)
	if host, err := os.Hostname(); err == nil {
		log = log.With("pod", host)
	}

	// 签名密钥没有默认值，也不允许自动生成：
	// 自动生成的随机密钥在重启后会让所有已发出的 token 全部失效，
	// 而 api 侧拿着旧密钥会验不过 —— 这种故障排查起来极其痛苦。
	signer, err := auth.NewSigner(cfg.JWTSecret, cfg.JWTIssuer, cfg.JWTTTL)
	if err != nil {
		log.Error("jwt_signer_init_failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := obs.InitTracing(ctx, "account", cfg.Version)
	if err != nil {
		log.Error("tracing_init_failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(sctx)
	}()

	st, err := store.Open(ctx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("database_open_failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// 表结构由 api 的迁移体系统一管。这里也跑一次是自举用的：
	// advisory lock 保证多进程同时启动时只有一个真的执行 DDL。
	applied, err := st.Migrate(ctx)
	if err != nil {
		log.Error("migration_failed", "err", err)
		os.Exit(1)
	}
	if len(applied) > 0 {
		log.Info("migrations_applied", "versions", applied)
	}

	host, _ := os.Hostname()
	srv := &account.Server{Log: log, Store: st, Signer: signer, Cfg: cfg, Pod: host}

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

	log.Info("account_listening", "addr", cfg.Addr, "token_ttl", signer.TTL().String())
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http_server_failed", "err", err)
		os.Exit(1)
	}
	log.Info("account_stopped")
}
