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
)

func Handler() http.Handler {
	return promhttp.Handler()
}
