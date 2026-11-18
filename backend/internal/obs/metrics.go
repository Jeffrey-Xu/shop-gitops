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

	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shop_queue_depth",
		Help: "主队列积压消息数（每次 /api/stats 或指标抓取时采样）",
	})
)

func Handler() http.Handler {
	return promhttp.Handler()
}
