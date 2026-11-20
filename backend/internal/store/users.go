package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var (
	ErrEmailTaken   = errors.New("email already registered")
	ErrUserNotFound = errors.New("user not found")
)

// User 是会回给客户端的结构 —— 注意它没有 password_hash 字段。
// 哈希永远不出这个包，连内部接口都不带出去，减少误序列化的机会。
type User struct {
	ID          int64     `json:"id"`
	Email       string    `json:"email"`
	DisplayName string    `json:"displayName"`
	CreatedAt   time.Time `json:"createdAt"`
}

type CreateUserParams struct {
	Email        string
	PasswordHash string
	DisplayName  string
}

// CreateUser 用 ON CONFLICT DO NOTHING + RETURNING 判重，而不是先 SELECT 再 INSERT。
// 后者在并发注册同一邮箱时两个请求都会通过检查，然后其中一个撞唯一索引报 500。
// 让数据库来做这件事，冲突就是「没有返回行」，语义明确。
func (s *Store) CreateUser(ctx context.Context, p CreateUserParams) (User, error) {
	var u User
	err := s.DB.QueryRowContext(ctx, `
		INSERT INTO users (email, password_hash, display_name)
		VALUES ($1, $2, $3)
		ON CONFLICT (lower(email)) DO NOTHING
		RETURNING id, email, display_name, created_at`,
		p.Email, p.PasswordHash, p.DisplayName).
		Scan(&u.ID, &u.Email, &u.DisplayName, &u.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrEmailTaken
	}
	if err != nil {
		return User{}, err
	}
	return u, nil
}

// GetUserAuthByEmail 是唯一会把哈希读出来的入口，只给登录用。
func (s *Store) GetUserAuthByEmail(ctx context.Context, email string) (User, string, error) {
	var (
		u    User
		hash string
	)
	err := s.DB.QueryRowContext(ctx, `
		SELECT id, email, display_name, created_at, password_hash
		  FROM users WHERE lower(email) = lower($1)`, email).
		Scan(&u.ID, &u.Email, &u.DisplayName, &u.CreatedAt, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, "", ErrUserNotFound
	}
	if err != nil {
		return User{}, "", err
	}
	return u, hash, nil
}

func (s *Store) GetUserByID(ctx context.Context, id int64) (User, error) {
	var u User
	err := s.DB.QueryRowContext(ctx, `
		SELECT id, email, display_name, created_at
		  FROM users WHERE id = $1`, id).
		Scan(&u.ID, &u.Email, &u.DisplayName, &u.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrUserNotFound
	}
	return u, err
}
