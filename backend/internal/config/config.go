// Package config 负责从环境变量装载配置。
// 原则：能给合理默认值就给，但连接串这类必填项缺失时立刻报错退出，
// 而不是等到第一个请求才炸。
package config

import (
	"fmt"
	"net/url"
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

	// ---- 数据库连接池 ----
	// 这几个值的默认值就是「本地单副本够用、压测要调」的位置，
	// 所以必须能被部署侧的 ConfigMap 覆盖，不能写死在代码里。
	DBMaxOpen         int
	DBMaxIdle         int
	DBConnMaxLifetime time.Duration
	// DBStatementTimeout 是每条 SQL 的服务端超时。
	// 没有它的话，一条卡住的查询会一直占着池里的连接：
	// 池只有 10 个，十条慢查询就能把整个服务堵死，且没有任何报错。
	DBStatementTimeout time.Duration
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

	connLifetime, err := time.ParseDuration(env("DB_CONN_MAX_LIFETIME", "5m"))
	if err != nil {
		return Config{}, fmt.Errorf("DB_CONN_MAX_LIFETIME 无法解析: %w", err)
	}

	stmtTimeout, err := time.ParseDuration(env("DB_STATEMENT_TIMEOUT", "10s"))
	if err != nil {
		return Config{}, fmt.Errorf("DB_STATEMENT_TIMEOUT 无法解析: %w", err)
	}

	dbMaxOpen := atoi(env("DB_MAX_OPEN_CONNS", "10"), 10)
	// 默认让 idle 跟随 open：两者不相等时，多余的连接会被反复关掉再建。
	// 显式配了 DB_MAX_IDLE_CONNS 才允许它更小（例如为了少占宿主的连接槽）。
	dbMaxIdle := atoi(env("DB_MAX_IDLE_CONNS", strconv.Itoa(dbMaxOpen)), dbMaxOpen)
	if dbMaxIdle > dbMaxOpen {
		return Config{}, fmt.Errorf("DB_MAX_IDLE_CONNS(%d) 不能大于 DB_MAX_OPEN_CONNS(%d)", dbMaxIdle, dbMaxOpen)
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

		DBMaxOpen:          dbMaxOpen,
		DBMaxIdle:          dbMaxIdle,
		DBConnMaxLifetime:  connLifetime,
		DBStatementTimeout: stmtTimeout,
	}

	if c.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL 为空")
	}
	// 运行时参数必须在拿到 service 名之后才能注入，所以放在结构体之后。
	if c.DatabaseURL, err = withDBParams(c.DatabaseURL, service, stmtTimeout); err != nil {
		return Config{}, err
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

// withDBParams 往连接串里注入两个「不写就永远查不到」的运行时参数：
//
//   - application_name：pg_stat_activity / pg_stat_statements 里能分清
//     哪个连接属于 api、哪个属于 worker。缺了它，线上只能看到一堆
//     shop 用户的长连接，谁在跑慢查询全靠猜。
//   - statement_timeout：服务端的单语句超时。池只有 10 个连接，
//     一条卡住的查询会一直占着不还 —— 十条就能把服务堵死，而且不报错。
//
// 用 url 解析而不是字符串拼接：口令里可能出现 @ : / ?，
// 拼字符串会把它们和新参数搅成一条解析不了的 URL。
// key=value 形式的 DSN（没有 scheme）原样返回，那种格式不在本项目里用。
func withDBParams(raw, service string, timeout time.Duration) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("DATABASE_URL 无法解析: %w", err)
	}
	if u.Scheme == "" {
		return raw, nil
	}
	q := u.Query()
	q.Set("application_name", service)
	if timeout > 0 {
		// PostgreSQL 的 statement_timeout 接受裸整数（单位毫秒）
		q.Set("statement_timeout", strconv.FormatInt(timeout.Milliseconds(), 10))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func atoi(s string, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
