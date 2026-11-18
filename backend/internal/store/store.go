package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const migrationLock int64 = 8891001

var (
	ErrProductNotFound = errors.New("product not found")
	ErrOutOfStock      = errors.New("out of stock")
	ErrOrderNotFound   = errors.New("order not found")
)

type Store struct {
	DB *sql.DB
}

func Open(ctx context.Context, dsn string, log *slog.Logger) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)

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
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("database unreachable after retries: %w", lastErr)
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
