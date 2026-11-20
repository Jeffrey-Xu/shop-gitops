// Package api 是 HTTP 入口：读走缓存，写走事务，事件进 outbox。
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/auth"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/broker"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/cache"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/store"
)

type Server struct {
	Log    *slog.Logger
	Store  *store.Store
	Cache  *cache.Cache
	Broker *broker.Broker
	Cfg    config.Config
	Pod    string
	// Signer 为 nil 表示没配 JWT_SECRET，此时所有请求都按匿名处理。
	// 这样本地只跑 api+worker 时不需要账号服务也能下单。
	Signer *auth.Signer
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /metrics", obs.Handler())

	mux.HandleFunc("GET /api/hello", s.handleHello)
	mux.HandleFunc("GET /api/products", s.handleProducts)
	mux.HandleFunc("GET /api/products/{id}", s.handleProduct)
	mux.HandleFunc("POST /api/orders", s.handleCreateOrder)
	mux.HandleFunc("GET /api/orders/{id}", s.handleGetOrder)
	mux.HandleFunc("GET /api/stats", s.handleStats)
	mux.HandleFunc("POST /api/dev/fault", s.handleFault)
	// 服务间端点：只给履约服务回写发货状态用，靠共享密钥鉴权。
	// 生产环境应该在 Ingress 层再拦一次 /api/internal/*，别只依赖应用层。
	mux.HandleFunc("POST /api/internal/shipments", s.requireInternal(s.handleShipmentUpdate))

	return s.withOptionalAuth(mux)
}

// withOptionalAuth 是「可选登录」：
//   - 没带 Authorization → 匿名放行（未登录也能下单）
//   - 带了但无效/过期   → 401，不能悄悄降级成匿名，
//     否则用户以为自己是登录状态下的单，实际订单不归属他
func (s *Server) withOptionalAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if h == "" || s.Signer == nil {
			next.ServeHTTP(w, r)
			return
		}
		claims, err := s.Signer.Verify(auth.BearerToken(h))
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "token 无效或已过期"})
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithClaims(r.Context(), claims)))
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "pod": s.Pod})
}

// readyz 才检查依赖：依赖挂了应该被摘流量，而不是被反复重启。
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.Store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "db": err.Error()})
		return
	}
	if err := s.Cache.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "redis": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": true, "pod": s.Pod})
}

func (s *Server) handleHello(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "api",
		"pod":     s.Pod,
		"version": s.Cfg.Version,
		"path":    r.URL.Path,
	})
}

// handleProducts 是 cache-aside 的教科书形状：
// 先查缓存 → 未命中回源 → 写回缓存。并发未命中由 singleflight 合并。
func (s *Server) handleProducts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	var items []store.Product
	hit, err := s.Cache.GetJSON(ctx, cache.KeyProductList, &items)
	if err != nil {
		s.Log.Warn("cache_get_failed", "key", cache.KeyProductList, "err", err)
	}
	if hit {
		s.markCache(ctx, w, "hit")
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "pod": s.Pod, "cache": "hit"})
		return
	}

	v, err := s.Cache.Do(cache.KeyProductList, func() (any, error) {
		list, err := s.Store.ListProducts(ctx)
		if err != nil {
			return nil, err
		}
		// 用 WithoutCancel：即使首个请求的客户端断开，也把结果写回缓存，
		// 否则这次回源就白跑了。
		_ = s.Cache.SetJSON(context.WithoutCancel(ctx), cache.KeyProductList, list)
		return list, nil
	})
	if err != nil {
		s.Log.Error("list_products_failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "查询商品失败"})
		return
	}

	list, ok := v.([]store.Product)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "缓存类型异常"})
		return
	}
	s.markCache(ctx, w, "miss")
	writeJSON(w, http.StatusOK, map[string]any{"items": list, "pod": s.Pod, "cache": "miss"})
}

func (s *Server) handleProduct(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "商品 id 非法"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	key := cache.KeyProduct(id)
	var p store.Product
	hit, err := s.Cache.GetJSON(ctx, key, &p)
	if err != nil {
		s.Log.Warn("cache_get_failed", "key", key, "err", err)
	}
	if hit {
		s.markCache(ctx, w, "hit")
		writeJSON(w, http.StatusOK, map[string]any{"item": p, "cache": "hit"})
		return
	}

	p, err = s.Store.GetProduct(ctx, id)
	if errors.Is(err, store.ErrProductNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "商品不存在"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "查询商品失败"})
		return
	}
	_ = s.Cache.SetJSON(ctx, key, p)
	s.markCache(ctx, w, "miss")
	writeJSON(w, http.StatusOK, map[string]any{"item": p, "cache": "miss"})
}

func (s *Server) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProductID int64 `json:"productId"`
		Quantity  int   `json:"quantity"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求体不是合法 JSON"})
		return
	}
	if req.Quantity <= 0 {
		req.Quantity = 1
	}
	if req.Quantity > 99 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "单次最多购买 99 件"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// 登录了就归属到人，没登录就匿名。不强制登录是刻意的：
	// 电商的首单转化率很大一部分来自「不用注册就能买」。
	var userID int64
	if c, ok := auth.FromContext(r.Context()); ok {
		userID = c.UserID
	}

	order, created, err := s.Store.CreateOrder(ctx, store.CreateOrderParams{
		ProductID:      req.ProductID,
		Quantity:       req.Quantity,
		IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
		UserID:         userID,
	})
	switch {
	case errors.Is(err, store.ErrProductNotFound):
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "商品不存在"})
		return
	case errors.Is(err, store.ErrOutOfStock):
		obs.OrdersCreated.WithLabelValues("out_of_stock").Inc()
		writeJSON(w, http.StatusConflict, map[string]any{"error": "库存不足", "pod": s.Pod})
		return
	case err != nil:
		s.Log.Error("create_order_failed", "err", err, "product_id", req.ProductID, "quantity", req.Quantity)
		obs.OrdersCreated.WithLabelValues("error").Inc()
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "下单失败"})
		return
	}

	// 库存变了，列表和单品缓存必须立刻失效。
	// 删缓存失败不回滚订单：数据库是对的，缓存最多脏一个 TTL。
	keys := []string{cache.KeyProductList}
	for _, it := range order.Items {
		keys = append(keys, cache.KeyProduct(it.ProductID))
	}
	if err := s.Cache.Del(context.WithoutCancel(ctx), keys...); err != nil {
		s.Log.Warn("cache_invalidate_failed", "keys", keys, "err", err)
	}

	code := http.StatusCreated
	if !created {
		// 幂等键命中：返回同一张订单，不产生新库存扣减
		code = http.StatusOK
		obs.OrdersCreated.WithLabelValues("duplicate").Inc()
	} else {
		obs.OrdersCreated.WithLabelValues("created").Inc()
	}
	writeJSON(w, code, order)
}

func (s *Server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "订单 id 非法"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	order, err := s.Store.GetOrder(ctx, id)
	if errors.Is(err, store.ErrOrderNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "订单不存在"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "查询订单失败"})
		return
	}

	// 防越权（IDOR）：订单只要归属了某个用户，就只有他能查。
	// 匿名订单（user_id 为空）保持公开 —— 下单时不登录、拿链接回来查的场景。
	// 返回 404 而不是 403：403 等于告诉对方「这个 id 存在但不是你的」。
	if order.UserID != 0 {
		c, ok := auth.FromContext(r.Context())
		if !ok || c.UserID != order.UserID {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "订单不存在"})
			return
		}
	}
	writeJSON(w, http.StatusOK, order)
}

// requireInternal 校验服务间共享密钥。
//
// 用 subtle.ConstantTimeCompare 而不是 == ：字符串比较会在第一个不同字节返回，
// 攻击者靠响应时间就能一位一位地把密钥猜出来。
func (s *Server) requireInternal(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 没配密钥就当端点不存在，避免「空 == 空」意外放行
		if s.Cfg.InternalToken == "" {
			http.NotFound(w, r)
			return
		}
		got := r.Header.Get("X-Internal-Token")
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.Cfg.InternalToken)) != 1 {
			s.Log.Warn("internal_auth_failed", "path", r.URL.Path, "remote", r.RemoteAddr)
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

// handleShipmentUpdate 是履约服务的唯一写入口。
// 状态机的合法性在这里判定，Node 侧只负责「我认为该发货了/该签收了」。
func (s *Server) handleShipmentUpdate(w http.ResponseWriter, r *http.Request) {
	var req store.ShipmentUpdate
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求体不是合法 JSON"})
		return
	}
	if req.OrderID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "orderId 必填"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	result, err := s.Store.AdvanceShipment(ctx, req)
	switch {
	case errors.Is(err, store.ErrIllegalTransition):
		obs.ShipmentCallbacks.WithLabelValues("invalid", req.Status).Inc()
		s.Log.Warn("illegal_shipment_transition", "order_id", req.OrderID, "to", req.Status)
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "当前状态不允许这个迁移", "orderId": req.OrderID, "status": req.Status,
		})
		return
	case errors.Is(err, store.ErrOrderNotFound):
		obs.ShipmentCallbacks.WithLabelValues("not_found", req.Status).Inc()
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "订单不存在"})
		return
	case err != nil:
		obs.ShipmentCallbacks.WithLabelValues("error", req.Status).Inc()
		s.Log.Error("advance_shipment_failed", "err", err, "order_id", req.OrderID)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "回写失败"})
		return
	}

	obs.ShipmentCallbacks.WithLabelValues(result, req.Status).Inc()
	s.Log.Info("shipment_advanced", "order_id", req.OrderID, "status", req.Status, "result", result)
	writeJSON(w, http.StatusOK, map[string]any{"result": result, "orderId": req.OrderID})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	depth, err := s.Broker.QueueDepth(broker.QueueOrderCreated)
	if err != nil {
		s.Log.Warn("queue_depth_failed", "err", err)
	}
	obs.QueueDepth.Set(float64(depth))

	orders, err := s.Store.OrderStats(ctx)
	if err != nil {
		s.Log.Warn("order_stats_failed", "err", err)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"cache": map[string]int64{
			"hit":  s.Cache.GetInt(ctx, "shop:stats:cache:hit"),
			"miss": s.Cache.GetInt(ctx, "shop:stats:cache:miss"),
		},
		"queue": map[string]any{
			"name":  broker.QueueOrderCreated,
			"depth": depth,
		},
		"orders": orders,
		"pod":    s.Pod,
	})
}

// handleFault 只用于演示：在 Grafana 里看错误率和日志链路。
func (s *Server) handleFault(w http.ResponseWriter, r *http.Request) {
	if !s.Cfg.DevMode {
		http.NotFound(w, r)
		return
	}
	switch r.URL.Query().Get("type") {
	case "panic":
		panic("demo: injected panic")
	case "slow":
		select {
		case <-time.After(3 * time.Second):
		case <-r.Context().Done():
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"result": "slow done"})
	case "error":
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "demo: injected error"})
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "type 取值 panic | slow | error"})
	}
}

func (s *Server) markCache(ctx context.Context, w http.ResponseWriter, result string) {
	w.Header().Set("X-Cache", strings.ToUpper(result))
	obs.CacheLookups.WithLabelValues(result).Inc()
	s.Cache.Incr(ctx, "shop:stats:cache:"+result)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
