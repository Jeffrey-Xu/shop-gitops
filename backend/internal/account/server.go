// Package account 是账号服务：注册、登录、查自己。
//
// 它只做三件事，边界刻意画得很死：
//   - 管密码（bcrypt 哈希，明文只在这一次请求的内存里存在）
//   - 签发 JWT（api 本地验签，不需要每次请求回查这里）
//   - 读用户资料（供展示）
//
// 它不管权限、不管订单归属、不管会话列表 —— 那些属于业务侧。
package account

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/auth"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/store"
)

const (
	minPasswordLen = 8
	// bcrypt 只吃前 72 字节，超长会直接被库里截断（而且新版库会报错）。
	// 与其让用户以为设了 200 位密码，不如早点拒绝。
	maxPasswordLen = 72
	maxBodyBytes   = 4 << 10
)

type Server struct {
	Log    *slog.Logger
	Store  *store.Store
	Signer *auth.Signer
	Cfg    config.Config
	Pod    string
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /metrics", obs.Handler())

	mux.HandleFunc("POST /register", s.handleRegister)
	mux.HandleFunc("POST /login", s.handleLogin)
	mux.HandleFunc("GET /me", s.requireAuth(s.handleMe))
	// 内部给前端/网关探活用的公开信息，不含任何用户数据
	mux.HandleFunc("GET /", s.handleInfo)

	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "account", "pod": s.Pod})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "db": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": true})
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "account",
		"version": s.Cfg.Version,
		"endpoints": []string{
			"POST /register", "POST /login", "GET /me",
		},
	})
}

type registerRequest struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	DisplayName string `json:"displayName"`
}

type authResponse struct {
	Token     string     `json:"token"`
	ExpiresAt time.Time  `json:"expiresAt"`
	User      store.User `json:"user"`
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	email := normalizeEmail(req.Email)
	if !looksLikeEmail(email) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "邮箱格式不正确"})
		obs.AuthEvents.WithLabelValues("register", "bad_input").Inc()
		return
	}
	if len(req.Password) < minPasswordLen {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "密码至少 8 位"})
		obs.AuthEvents.WithLabelValues("register", "bad_input").Inc()
		return
	}
	if len(req.Password) > maxPasswordLen {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "密码最多 72 字节"})
		obs.AuthEvents.WithLabelValues("register", "bad_input").Inc()
		return
	}

	// cost=10 约 50~80ms。调到 12 会让登录接口变成天然的压测瓶颈，
	// 而这套系统的瓶颈本该在数据库和 broker 上。
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		s.Log.Error("password_hash_failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "注册失败"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	name := strings.TrimSpace(req.DisplayName)
	if name == "" {
		// 默认用邮箱 @ 前面那截做昵称，省得前端再问一次
		if at := strings.IndexByte(email, '@'); at > 0 {
			name = email[:at]
		}
	}

	user, err := s.Store.CreateUser(ctx, store.CreateUserParams{
		Email:        email,
		PasswordHash: string(hash),
		DisplayName:  name,
	})
	switch {
	case errors.Is(err, store.ErrEmailTaken):
		obs.AuthEvents.WithLabelValues("register", "email_taken").Inc()
		writeJSON(w, http.StatusConflict, map[string]any{"error": "该邮箱已注册"})
		return
	case err != nil:
		s.Log.Error("create_user_failed", "err", err)
		obs.AuthEvents.WithLabelValues("register", "error").Inc()
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "注册失败"})
		return
	}

	token, exp, err := s.Signer.Sign(user.ID, user.Email)
	if err != nil {
		s.Log.Error("sign_token_failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "注册成功但签发凭证失败"})
		return
	}

	obs.AuthEvents.WithLabelValues("register", "ok").Inc()
	s.Log.Info("user_registered", "user_id", user.ID, "email", user.Email)
	writeJSON(w, http.StatusCreated, authResponse{Token: token, ExpiresAt: exp, User: user})
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	email := normalizeEmail(req.Email)
	if email == "" || req.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "邮箱和密码不能为空"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	user, hash, err := s.Store.GetUserAuthByEmail(ctx, email)
	if errors.Is(err, store.ErrUserNotFound) {
		// 用户不存在时也跑一次 bcrypt：
		// 否则「不存在」1ms 返回、「密码错」60ms 返回，
		// 响应时间本身就把「这个邮箱有没有注册过」泄露了。
		_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(req.Password))
		obs.AuthEvents.WithLabelValues("login", "invalid").Inc()
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "邮箱或密码错误"})
		return
	}
	if err != nil {
		s.Log.Error("get_user_failed", "err", err)
		obs.AuthEvents.WithLabelValues("login", "error").Inc()
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "登录失败"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)); err != nil {
		obs.AuthEvents.WithLabelValues("login", "invalid").Inc()
		// 文案和「用户不存在」完全一致，不告诉对方是哪一半错了
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "邮箱或密码错误"})
		return
	}

	token, exp, err := s.Signer.Sign(user.ID, user.Email)
	if err != nil {
		s.Log.Error("sign_token_failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "登录失败"})
		return
	}

	obs.AuthEvents.WithLabelValues("login", "ok").Inc()
	s.Log.Info("user_logged_in", "user_id", user.ID)
	writeJSON(w, http.StatusOK, authResponse{Token: token, ExpiresAt: exp, User: user})
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request, claims *auth.Claims) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	user, err := s.Store.GetUserByID(ctx, claims.UserID)
	if errors.Is(err, store.ErrUserNotFound) {
		// token 有效但用户被删了 —— 让客户端清掉本地凭证
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "账号不存在"})
		return
	}
	if err != nil {
		s.Log.Error("get_user_failed", "err", err, "user_id", claims.UserID)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "查询失败"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": user})
}

// requireAuth 只服务账号服务自己的 /me。
// api 那边的鉴权是「可选」的（匿名也能下单），逻辑不同，别急着抽公共。
func (s *Server) requireAuth(next func(http.ResponseWriter, *http.Request, *auth.Claims)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := auth.BearerToken(r.Header.Get("Authorization"))
		if tok == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="shop"`)
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "缺少 Bearer token"})
			return
		}
		claims, err := s.Signer.Verify(tok)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="shop", error="invalid_token"`)
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "token 无效或已过期"})
			return
		}
		next(w, r, claims)
	}
}

// dummyHash 只在「用户不存在」分支用来消耗等量 CPU，永远不匹配任何输入。
// 启动时真实生成一个（约 60ms），而不是手抄一个字符串常量 ——
// 抄错的 hash 会让 bcrypt 立刻返回「invalid hash」，等于没做防时序。
var dummyHash = func() []byte {
	h, err := bcrypt.GenerateFromPassword([]byte("shop-account-dummy-password"), bcrypt.DefaultCost)
	if err != nil {
		return nil
	}
	return h
}()

func normalizeEmail(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// looksLikeEmail 刻意宽松：只挡明显的手误（无 @、有空格、没有点）。
// 真正的验证是「能不能收到信」，那是发信环节的事，不是正则的事。
func looksLikeEmail(s string) bool {
	if len(s) < 5 || len(s) > 254 {
		return false
	}
	if strings.ContainsAny(s, " \t\r\n") {
		return false
	}
	at := strings.LastIndexByte(s, '@')
	if at <= 0 || at == len(s)-1 {
		return false
	}
	domain := s[at+1:]
	return strings.Contains(domain, ".") && !strings.HasPrefix(domain, ".") && !strings.HasSuffix(domain, ".")
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求体不是合法 JSON"})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
