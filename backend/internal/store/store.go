package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/XSAM/otelsql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const migrationLock int64 = 8891001

var (
	ErrProductNotFound = errors.New("product not found")
	ErrOutOfStock      = errors.New("out of stock")
	ErrOrderNotFound   = errors.New("order not found")

	// 下单请求本身的形状问题。分开定义是为了让 API 层能回 400
	// 而不是把参数错误混进 500 里 —— 两者对调用方的含义完全不同。
	ErrEmptyCart       = errors.New("order has no lines")
	ErrInvalidProduct  = errors.New("invalid product id")
	ErrInvalidQuantity = errors.New("invalid quantity")
)

type Store struct {
	DB *sql.DB
}

// Open 建连接池。
//
// 池大小是参数而不是常量：压测时第一件要调的就是它，硬编码会把
// 「调参」变成「改代码 + 发版 + 等 CI」。
//
// 用 otelsql 包住 pgx 的 database/sql 驱动，而不是在各处手写 span：
// 一条 SQL 的耗时属于「谁都该有但谁都不想逐处写」的信息，
// 驱动层是唯一能一次覆盖全部查询位置的地方。
func Open(ctx context.Context, dsn string, log *slog.Logger, maxOpen, maxIdle int, maxLifetime time.Duration) (*Store, error) {
	db, err := otelsql.Open("pgx", dsn,
		otelsql.WithSpanNameFormatter(dbSpanName),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			// readyz 每 5 秒 ping 一次。给 ping 采 span 只会把 Tempo 灌满
			// 与业务无关的噪声，真出问题时反而找不到那一单。
			Ping: false,
			// 每个结果行一条 event 是 O(行数) 的噪声，一条几十行的查询
			// 就能把单条 trace 撑到上千个 event。
			RowsNext: false,
			// 连 sql.rows 这个 span 也不要。实测一单里有 6 个同名
			// sql.rows（QueryRow 也会产生），瀑布图上就是一片无信息量的
			// 细条，反而把 UPDATE products / INSERT outbox 这些真正的
			// 语句淹掉。本项目的结果集都在几十行内，迭代耗时没有排查价值。
			OmitRows:       true,
			DisableErrSkip: true,
			// prepare / reset_session 是连接池内部动作，不是业务交互。
			OmitConnPrepare:      true,
			OmitConnResetSession: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	// MaxIdle 必须等于 MaxOpen。默认 database/sql 是 2：
	// 空闲连接随时被关掉，下一个请求又要重新握手 ——
	// 握手（TCP + 认证 + 参数下发）比复用贵一个数量级，
	// 平时看不出，压测时就是白白多出来的一段延迟。
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(maxLifetime)
	// 空闲不回收：靠 MaxLifetime 定期轮换就够了，让池保持温热。
	db.SetConnMaxIdleTime(0)

	var lastErr error
	for i := 1; i <= 30; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = db.PingContext(pingCtx)
		cancel()
		if lastErr == nil {
			return &Store{DB: db}, nil
		}
		log.Warn("waiting_for_database", "attempt", i, "err", lastErr)
		select {
		case <-ctx.Done():
			_ = db.Close()
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	_ = db.Close()
	return nil, fmt.Errorf("database unreachable after retries: %w", lastErr)
}

// sqlTableRe 抓「操作动词 + 表名」里的表名。
// 只认 from/into/update/table 后面的第一个标识符，够覆盖本项目的全部语句。
var sqlTableRe = regexp.MustCompile(`(?i)\b(?:from|into|update|table)\s+([a-z_][a-z0-9_]*)`)

// dbSpanName 把 SQL 收敛成「操作 + 表名」，例如 SELECT orders、INSERT outbox。
//
// 不能拿整条 SQL 当 span 名：参数不同就是不同的名字，基数会随数据增长
// （WHERE id = 1 / = 2 / = 3 …），Tempo 里按名字聚合就失效了。
// 表名 + 操作是有限集合（本项目 9 张表 × 4 种操作），聚合和报警都能用。
func dbSpanName(_ context.Context, method otelsql.Method, query string) string {
	fields := strings.Fields(strings.TrimSpace(query))
	if len(fields) == 0 {
		// BEGIN / COMMIT 这类没有 SQL 文本，用 otelsql 的方法名。
		return string(method)
	}
	op := strings.ToUpper(fields[0])
	m := sqlTableRe.FindStringSubmatch(query)
	if len(m) < 2 {
		return op
	}
	return op + " " + strings.ToLower(m[1])
}

func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.DB.PingContext(ctx) }

// Migrate 在拿到 advisory lock 之后按文件名顺序执行未应用的迁移。
// 用 advisory lock 是因为将来 api 一旦 >1 副本，两个实例会同时启动；
// 没锁的话两个进程会同时跑 DDL 然后互相把对方锁死。
func (s *Store) Migrate(ctx context.Context) ([]string, error) {
	if _, err := s.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}

	conn, err := s.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLock); err != nil {
		return nil, fmt.Errorf("advisory lock: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLock)
	}()

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	var applied []string
	for _, e := range entries {
		name := e.Name()
		var exists bool
		if err := conn.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, name).Scan(&exists); err != nil {
			return applied, err
		}
		if exists {
			continue
		}

		body, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return applied, err
		}

		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return applied, err
		}
		// DDL 合法地可能比一次请求慢（建大索引、重写表结构），
		// 不该被请求级的 statement_timeout 掐掉。
		// 用 SET LOCAL 而不是 SET：前者只作用于本事务，提交即失效；
		// 用 SET 的话这条连接回池之后会带着「永不超时」去跑业务查询。
		if _, err := tx.ExecContext(ctx, `SET LOCAL statement_timeout = 0`); err != nil {
			_ = tx.Rollback()
			return applied, fmt.Errorf("migration %s: clear statement_timeout: %w", name, err)
		}
		for i, stmt := range splitStatements(string(body)) {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				_ = tx.Rollback()
				return applied, fmt.Errorf("migration %s stmt %d: %w", name, i+1, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback()
			return applied, err
		}
		if err := tx.Commit(); err != nil {
			return applied, err
		}
		applied = append(applied, name)
	}
	return applied, nil
}

// splitStatements 按行尾分号切分。
// pgx 走扩展协议，一条 Exec 里塞多条语句会直接报错，
// 所以迁移文件必须能被切成单条。约定：每条语句最后一个字符是行尾的 ";"
// （字符串常量里不出现分号结尾的行，本项目可控）。
func splitStatements(body string) []string {
	var out []string
	var cur []string
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		cur = append(cur, line)
		if strings.HasSuffix(trimmed, ";") {
			out = append(out, strings.Join(cur, "\n"))
			cur = nil
		}
	}
	if len(cur) > 0 {
		out = append(out, strings.Join(cur, "\n"))
	}
	return out
}
