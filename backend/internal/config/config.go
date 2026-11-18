// Package config 负责从环境变量装载配置。
// 原则：能给合理默认值就给，但连接串这类必填项缺失时立刻报错退出，
// 而不是等到第一个请求才炸。
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Service     string
	Addr        string
	DatabaseURL string
	RedisAddr   string
	RedisPass   string
	RabbitURL   string
	Version     string
	LogLevel    string
	CacheTTL    time.Duration
	DevMode     bool
	Prefetch    int
	MaxAttempts int
}

func Load(service string) (Config, error) {
	ttl, err := time.ParseDuration(env("CACHE_TTL", "30s"))
	if err != nil {
		return Config{}, fmt.Errorf("CACHE_TTL 无法解析: %w", err)
	}

	c := Config{
		Service:     service,
		Addr:        env("ADDR", ":8080"),
		DatabaseURL: env("DATABASE_URL", "postgres://shop:shop@127.0.0.1:5432/shop?sslmode=disable"),
		RedisAddr:   env("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPass:   os.Getenv("REDIS_PASSWORD"),
		// vhost 是 /shop，URL 里必须编码成 %2F，否则会被当成路径分隔符
		RabbitURL:   env("RABBITMQ_URL", "amqp://shop:shoppass@127.0.0.1:5672/%2Fshop"),
		Version:     env("APP_VERSION", "dev"),
		LogLevel:    env("LOG_LEVEL", "info"),
		CacheTTL:    ttl,
		DevMode:     env("DEV_MODE", "false") == "true",
		Prefetch:    atoi(env("MQ_PREFETCH", "5"), 5),
		MaxAttempts: atoi(env("MQ_MAX_ATTEMPTS", "3"), 3),
	}

	if c.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL 为空")
	}
	if c.RedisAddr == "" {
		return Config{}, fmt.Errorf("REDIS_ADDR 为空")
	}
	if c.RabbitURL == "" {
		return Config{}, fmt.Errorf("RABBITMQ_URL 为空")
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func atoi(s string, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
