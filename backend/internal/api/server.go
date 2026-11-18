// Package api 是 HTTP 入口：读走缓存，写走事务，事件进 outbox。
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

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

	return mux
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

	order, created, err := s.Store.CreateOrder(ctx, store.CreateOrderParams{
		ProductID:      req.ProductID,
		Quantity:       req.Quantity,
		IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
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
	writeJSON(w, http.StatusOK, order)
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
