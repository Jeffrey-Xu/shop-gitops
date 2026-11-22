// Package api 是 HTTP 入口：读走缓存，写走事务，事件进 outbox。
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
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

	// redisDown 记住上一次探针的结果，只为了「状态翻转时打一行日志」。
	// 放这里而不是局部变量：Server 是单实例，本来就只有一个探针序列。
	redisDown atomic.Bool
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
	// 注意顺序：Go 1.22 的 mux 按模式具体度匹配，/api/orders 和
	// /api/orders/{id} 不会互相抢，注册先后无所谓。
	mux.HandleFunc("GET /api/orders", s.requireAuth(s.handleMyOrders))
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

// requireAuth 是「必须登录」。它不自己解析 token ——
// 外面那层 withOptionalAuth 已经把合法 claims 放进 ctx 了，
// 这里只判断「有没有」。两处各解析一次容易写出不一致的判定。
func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := auth.FromContext(r.Context()); !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "需要登录"})
			return
		}
		next(w, r)
	}
}

// handleMyOrders 返回当前登录用户的订单。
//
// 只按 token 里的 uid 查，不接受 userId 查询参数 ——
// 收参数就等于给了越权入口，而「我的订单」根本不需要这个能力。
func (s *Server) handleMyOrders(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.FromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "需要登录"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}
	orders, err := s.Store.ListOrdersByUser(ctx, claims.UserID, limit)
	if err != nil {
		s.Log.Error("list_orders_failed", "err", err, "user_id", claims.UserID)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "查询订单失败"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": orders, "total": len(orders)})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "pod": s.Pod})
}

// readyz 才检查依赖：依赖挂了应该被摘流量，而不是被反复重启。
//
// 但「依赖」不是一个整体：PostgreSQL 和 Redis 在这个服务里的地位完全不同。
//
//   - PG 是硬依赖。库存、订单、outbox 全在里面，连不上几乎每个接口都会 500，
//     继续留在这个 Pod 上接流量只会把 500 放大。返回 503 让 K8s 摘掉它。
//     注意这里是 readiness 而不是 liveness：数据库慢的时候重启应用
//     只会让情况更糟（重启后还是要连同一个库）。
//   - Redis 是软依赖。它只承担商品目录缓存，下单链路一次都不读。
//     挂了正确行为是「穿透回源」——吞吐掉一截，但单还是能下。
//     如果把它当硬依赖，一次 Redis 抖动会让整个 api 被摘流量，
//     那就是把「降级」当成了「死亡」：明明能服务的，自己主动不服务了。
//
// 将来如果出现真正依赖 Redis 的强一致需求（分布式锁、限流器），
// 这段判断要跟着改 —— 依赖的强弱是由调用路径决定的，不是由组件决定的。
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.Store.Ping(ctx); err != nil {
		obs.DependencyUp.WithLabelValues("postgres", "hard").Set(0)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "db": err.Error()})
		return
	}
	obs.DependencyUp.WithLabelValues("postgres", "hard").Set(1)

	redisState := "up"
	if err := s.Cache.Ping(ctx); err != nil {
		redisState = "degraded"
		obs.DependencyUp.WithLabelValues("redis", "soft").Set(0)
		// 探针每 5 秒跑一次，只在「掉下去」的那一刻打日志。
		// 每次都打的话，Redis 挂一小时就是 720 行噪音，
		// 反而把真正有用的那行「什么时候开始挂的」埋了。
		if s.redisDown.CompareAndSwap(false, true) {
			obs.DependencyFlaps.WithLabelValues("redis").Inc()
			s.Log.Warn("redis_degraded",
				"err", err,
				"ready", true,
				"impact", "商品目录缓存穿透回源，下单与订单查询不受影响")
		}
	} else {
		obs.DependencyUp.WithLabelValues("redis", "soft").Set(1)
		if s.redisDown.CompareAndSwap(true, false) {
			s.Log.Info("redis_recovered")
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ready": true,
		"pod":   s.Pod,
		"redis": redisState,
	})
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
//
// 筛选和排序都在内存里做，不往 SQL 下推，原因是：
//   - 目录只有十几行，全量反序列化不到的耗时可忽略；
//   - 缓存键保持唯一（shop:products:list）。按 category/q 做缓存键会爆炸，
//     而且下单后的失效逻辑要跟着改（现在只删一个键）。
//
// 目录涨到几百行、或者要接搜索引擎时，这个决定就该反过来。
func (s *Server) handleProducts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	var items []store.Product
	hit, err := s.Cache.GetJSON(ctx, cache.KeyProductList, &items)
	if err != nil {
		s.Log.Warn("cache_get_failed", "key", cache.KeyProductList, "err", err)
	}
	cacheState := "hit"
	if !hit {
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
		items = list
		cacheState = "miss"
	}

	// 品类清单从全量数据算出来，不写死在前端 ——
	// 前端硬编码一份的话，加品类就得两边一起发版。
	counts := map[string]int{}
	for _, p := range items {
		counts[p.Category]++
	}
	categories := make([]map[string]any, 0, len(counts))
	for name, n := range counts {
		categories = append(categories, map[string]any{"name": name, "count": n})
	}
	// map 遍历顺序是随机的，数量相同的品类每次请求都会换位置，
	// 前端 chips 就会跳来跳去。必须给一个确定的全序：数量优先，其次名字。
	sort.Slice(categories, func(i, j int) bool {
		ci, cj := categories[i]["count"].(int), categories[j]["count"].(int)
		if ci != cj {
			return ci > cj
		}
		return categories[i]["name"].(string) < categories[j]["name"].(string)
	})

	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	cat := strings.TrimSpace(r.URL.Query().Get("category"))

	// 先按品类和关键词收窄，再排序：排序是对结果的排序，
	// 反过来的话每个排序分支都要重写一遍过滤逻辑。
	filtered := make([]store.Product, 0, len(items))
	for _, p := range items {
		if cat != "" && p.Category != cat {
			continue
		}
		if q != "" &&
			!strings.Contains(strings.ToLower(p.Name), q) &&
			!strings.Contains(strings.ToLower(p.Description), q) {
			continue
		}
		filtered = append(filtered, p)
	}

	switch r.URL.Query().Get("sort") {
	case "price_asc":
		sort.SliceStable(filtered, func(i, j int) bool {
			return filtered[i].PriceCents < filtered[j].PriceCents
		})
	case "price_desc":
		sort.SliceStable(filtered, func(i, j int) bool {
			return filtered[i].PriceCents > filtered[j].PriceCents
		})
	case "sales":
		sort.SliceStable(filtered, func(i, j int) bool {
			return filtered[i].Sold > filtered[j].Sold
		})
	case "rating":
		sort.SliceStable(filtered, func(i, j int) bool {
			return filtered[i].Rating > filtered[j].Rating
		})
	}

	s.markCache(ctx, w, cacheState)
	writeJSON(w, http.StatusOK, map[string]any{
		"items":      filtered,
		"total":      len(filtered),
		"catalog":    len(items),
		"categories": categories,
		"pod":        s.Pod,
		"cache":      cacheState,
	})
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
	// 同时接受两种请求体：
	//   {"items":[{"productId":1,"quantity":2}, ...]}   一单多品
	//   {"productId":1,"quantity":2}                    旧的单商品格式
	// 老前端在滚动更新期间还会打后者，直接拒绝会砸出 400。
	var req struct {
		Items     []store.OrderLine `json:"items"`
		ProductID int64             `json:"productId"`
		Quantity  int               `json:"quantity"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "请求体不是合法 JSON"})
		return
	}
	if len(req.Items) == 0 && req.Quantity <= 0 {
		req.Quantity = 1
	}
	// 单行上限 99：这不是业务规则，是防止有人一个请求把库存扫空。
	for _, it := range req.Items {
		if it.Quantity > 99 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "单件商品最多购买 99 件"})
			return
		}
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
		Items:          req.Items,
		ProductID:      req.ProductID,
		Quantity:       req.Quantity,
		IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
		UserID:         userID,
	})
	switch {
	case errors.Is(err, store.ErrEmptyCart),
		errors.Is(err, store.ErrInvalidProduct),
		errors.Is(err, store.ErrInvalidQuantity):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "订单内容不合法"})
		return
	case errors.Is(err, store.ErrProductNotFound):
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "商品不存在"})
		return
	case errors.Is(err, store.ErrOutOfStock):
		obs.OrdersCreated.WithLabelValues("out_of_stock").Inc()
		writeJSON(w, http.StatusConflict, map[string]any{"error": "库存不足", "pod": s.Pod})
		return
	case err != nil:
		s.Log.Error("create_order_failed", "err", err,
			"product_id", req.ProductID, "quantity", req.Quantity, "lines", len(req.Items))
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

	depths := make(map[string]int, len(broker.AllQueues))
	for _, q := range broker.AllQueues {
		d, err := s.Broker.QueueDepth(q)
		if err != nil {
			s.Log.Warn("queue_depth_failed", "queue", q, "err", err)
			continue
		}
		depths[q] = d
		obs.QueueMessages.WithLabelValues(q).Set(float64(d))
	}

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
			"depth": depths[broker.QueueOrderCreated],
			// 两个 DLQ 的和：只要不是 0 就有订单卡住了，需要人看
			"dlq": depths[broker.QueueOrderDLQ] + depths[broker.QueueOrderPaidDLQ],
			"all": depths,
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
