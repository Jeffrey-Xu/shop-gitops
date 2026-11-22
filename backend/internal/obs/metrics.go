package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// 指标命名遵循 Prometheus 惯例：<namespace>_<subsystem>_<name>_<unit>
// 标签只放低基数字段，route 用的是 mux 的 pattern（/api/orders/{id}），
// 不是真实路径，否则每个订单 id 都会变成一条新时间序列。
var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shop_http_requests_total",
		Help: "HTTP 请求总数",
	}, []string{"route", "method", "status"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "shop_http_request_duration_seconds",
		Help:    "HTTP 请求耗时",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"route", "method"})

	CacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shop_cache_lookups_total",
		Help: "缓存查询次数",
	}, []string{"result"})

	OrdersCreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shop_orders_created_total",
		Help: "下单次数",
	}, []string{"result"})

	OrdersProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shop_orders_processed_total",
		Help: "worker 处理结果",
	}, []string{"result"})

	OutboxPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shop_outbox_published_total",
		Help: "outbox relay 投递结果",
	}, []string{"result"})

	// result: succeeded / declined / error / disabled
	// declined 与 error 的区别很关键：前者是终态（卡被拒），后者值得重试
	PaymentCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shop_payment_calls_total",
		Help: "worker 调用支付网关的结果",
	}, []string{"result"})

	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shop_queue_depth",
		Help: "主队列积压消息数（每 15s 采样一次）",
	})

	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shop_outbox_pending",
		Help: "已写入但尚未投递的 outbox 行数（每 15s 采样一次）",
	})

	// action: register / login；result: ok / invalid / email_taken / bad_input / error
	AuthEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shop_auth_events_total",
		Help: "账号服务的注册与登录结果",
	}, []string{"action", "result"})

	// 履约服务回调 api 回写发货状态的结果。
	// result: ok / duplicate（已经推进过，幂等命中）/ invalid（状态机不允许）/ error
	ShipmentCallbacks = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shop_shipment_callbacks_total",
		Help: "api 接收履约服务回写的次数",
	}, []string{"result", "status"})

	// 依赖探活：1 可用 / 0 不可用。
	// mode 标出这个依赖挂了意味着什么：hard = Pod 被摘流量（服务不可用），
	// soft = 只是降级（仍能接单，某个能力变慢）。同一个 0，两种含义，
	// 混在一起看会误判事故等级。
	DependencyUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shop_dependency_up",
		Help: "依赖探活结果：1 可用 / 0 不可用",
	}, []string{"dependency", "mode"})

	// 软依赖从可用掉到降级的次数。只在状态翻转时 +1，不是每次探针都加 ——
	// 否则就是一条随时间线性上升的斜线，看不出「挂了几次、什么时候挂的」。
	DependencyFlaps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shop_dependency_flaps_total",
		Help: "依赖从可用变成不可用的次数",
	}, []string{"dependency"})
)

func Handler() http.Handler {
	return promhttp.Handler()
}
