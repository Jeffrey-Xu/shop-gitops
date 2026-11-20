// Package auth 负责签发与校验 JWT。
//
// 为什么是 JWT 而不是「每次请求回查账号服务」：
//   - api 每个请求都要知道「这是谁」，多一跳网络就多一份延迟和故障面
//   - HS256 对称密钥，api 本地验签，账号服务只在登录时出现
//
// 代价是 token 无法主动吊销。所以 TTL 给短（默认 24h），
// 真要吊销就得引入 jti 黑名单或换 RS256 + 短期 access + refresh。
package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidToken = errors.New("token 无效")
	ErrExpiredToken = errors.New("token 已过期")
)

// Claims 是塞进 token 的内容。只放低敏字段：
// uid 用来关联订单，email 用来展示。绝不放密码哈希、角色以外的东西。
type Claims struct {
	UserID int64  `json:"uid"`
	Email  string `json:"email"`
	jwt.RegisteredClaims
}

type Signer struct {
	secret []byte
	issuer string
	ttl    time.Duration
}

// NewSigner 的 secret 太短直接拒绝：HS256 的安全性完全押在这个值上，
// 一个 "secret" 或者空串等于没有签名。
func NewSigner(secret, issuer string, ttl time.Duration) (*Signer, error) {
	if len(secret) < 16 {
		return nil, fmt.Errorf("JWT_SECRET 至少 16 字节，当前 %d", len(secret))
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Signer{secret: []byte(secret), issuer: issuer, ttl: ttl}, nil
}

func (s *Signer) TTL() time.Duration { return s.ttl }

func (s *Signer) Sign(userID int64, email string) (string, time.Time, error) {
	now := time.Now()
	exp := now.Add(s.ttl)
	claims := Claims{
		UserID: userID,
		Email:  email,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   fmt.Sprintf("%d", userID),
			Issuer:    s.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
	if err != nil {
		return "", time.Time{}, err
	}
	return tok, exp, nil
}

// Verify 显式限定签名算法。不加这一行就有 alg=none 攻击：
// 攻击者把 header 改成 {"alg":"none"} 并去掉签名，若库信任 header 就会放行。
func (s *Signer) Verify(token string) (*Claims, error) {
	claims := &Claims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("非预期的签名算法: %v", t.Header["alg"])
		}
		return s.secret, nil
	}, jwt.WithIssuer(s.issuer), jwt.WithValidMethods([]string{"HS256"}))
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, ErrInvalidToken
	}
	if !parsed.Valid {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// ---------------------------------------------------------------------------
// context 传递：handler 里不用层层透传 claims
// ---------------------------------------------------------------------------

type ctxKey struct{}

func WithClaims(ctx context.Context, c *Claims) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

func FromContext(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(ctxKey{}).(*Claims)
	return c, ok
}

// BearerToken 从 Authorization 头里取 token。
// 大小写不敏感：客户端写 bearer / Bearer 都见过。
func BearerToken(header string) string {
	const prefix = "bearer "
	h := strings.TrimSpace(header)
	if len(h) <= len(prefix) {
		return ""
	}
	if !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}
