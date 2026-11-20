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
	// PaymentURL 为空表示不接真实支付网关，worker 退回内置的模拟支付。
	// 集群里指向 http://payment:8080（同命名空间的 Service）。
	PaymentURL     string
	PaymentTimeout time.Duration

	// JWTSecret 由 account 签发、api 验签，两边必须是同一个值。
	// 留空时 account 会拒绝启动（签名密钥不能有默认值），api 则降级为「不鉴权」。
	JWTSecret string
	JWTIssuer string
	JWTTTL    time.Duration
	// InternalToken 用于服务间调用的共享密钥（履约服务 → api 回写发货状态）。
	// 和 JWT 分开：机器身份和用户身份是两件事，泄露一个不该连带另一个。
	InternalToken string
}

func Load(service string) (Config, error) {
	ttl, err := time.ParseDuration(env("CACHE_TTL", "30s"))
	if err != nil {
		return Config{}, fmt.Errorf("CACHE_TTL 无法解析: %w", err)
	}

	payTimeout, err := time.ParseDuration(env("PAYMENT_TIMEOUT", "3s"))
	if err != nil {
		return Config{}, fmt.Errorf("PAYMENT_TIMEOUT 无法解析: %w", err)
	}

	jwtTTL, err := time.ParseDuration(env("JWT_TTL", "24h"))
	if err != nil {
		return Config{}, fmt.Errorf("JWT_TTL 无法解析: %w", err)
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
		// 故意不给默认值：没设就是「不接网关」，走模拟支付。
		// 给了默认值会让本地跑的时候意外去连一个不存在的地址。
		PaymentURL:     os.Getenv("PAYMENT_URL"),
		PaymentTimeout: payTimeout,

		JWTSecret:     os.Getenv("JWT_SECRET"),
		JWTIssuer:     env("JWT_ISSUER", "shop"),
		JWTTTL:        jwtTTL,
		InternalToken: os.Getenv("INTERNAL_TOKEN"),
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
