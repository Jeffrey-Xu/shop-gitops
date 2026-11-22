// Package httpx 放 HTTP 中间件：request id、访问日志、panic 兜底、指标。
package httpx

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/obs"
)

type ridKey struct{}

// RequestID 从 ctx 取出请求 id，handler 里打进日志，
// 就能用一条 id 把 access log、业务日志、错误日志串起来。
func RequestID(ctx context.Context) string {
	if v, ok := ctx.Value(ridKey{}).(string); ok {
		return v
	}
	return ""
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

// Wrap 一次性套上 request id / panic recover / 指标 / 访问日志。
// 探针和 /metrics 的请求不写访问日志，否则 Loki 里全是噪音。
func Wrap(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		rid := r.Header.Get("X-Request-ID")
		if rid == "" {
			rid = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", rid)

		// 探针和 /metrics 不建 span：kubelet 每 10s 探一次、Prometheus 每 15s 抓一次，
		// 全采的话 Tempo 里绝大多数是这两类，真正要看的订单链路会被埋掉。
		skipTrace := r.URL.Path == "/metrics" || r.URL.Path == "/healthz" || r.URL.Path == "/readyz"

		traceCtx := r.Context()
		var span trace.Span
		if !skipTrace {
			// Extract 以 r.Context() 为基底：上游传来的 traceparent 会被接上，
			// 同时这个 ctx 上的取消信号和已有的 value 都不会丢。
			traceCtx = otel.GetTextMapPropagator().Extract(traceCtx, propagation.HeaderCarrier(r.Header))
			traceCtx, span = obs.Tracer().Start(traceCtx, "http.request",
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					attribute.String("http.request.method", r.Method),
					attribute.String("url.path", r.URL.Path),
				),
			)
		}

		ctx := context.WithValue(traceCtx, ridKey{}, rid)
		r2 := r.WithContext(ctx)
		// 必须用局部变量。直接 log = log.With(...) 会在闭包里累积，
		// 每个请求再加一层 request_id，并发时还会互相踩（data race）。
		// trace_id 只在真的有 span 时才加：写个空字符串进 Loki，
		// 按 trace_id 过滤时会混进一堆无关的行。
		logArgs := []any{"request_id", rid}
		if tid := obs.TraceID(ctx); tid != "" {
			logArgs = append(logArgs, "trace_id", tid)
		}
		reqLog := log.With(logArgs...)

		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			if rec := recover(); rec != nil {
				reqLog.Error("panic_recovered",
					"err", fmt.Sprint(rec),
					"path", r2.URL.Path,
					"stack", string(debug.Stack()),
				)
				if sw.status == 0 {
					http.Error(sw, `{"error":"internal error"}`, http.StatusInternalServerError)
				}
			}

			// r2.Pattern 由 ServeMux 在匹配后写入，这里已经能拿到稳定模板
			route := r2.Pattern
			if route == "" {
				route = "unmatched"
			}
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}
			elapsed := time.Since(start)

			if span != nil {
				// 名字要等 ServeMux 匹配完才知道（r2.Pattern 是此刻才写好的），
				// 所以先建 span、最后改名。否则 Tempo 里全是 "http.request"，
				// 分不清是 /api/orders 还是 /api/products。
				if route == "unmatched" {
					span.SetName(r2.Method + " " + r2.URL.Path)
				} else {
					span.SetName(route)
				}
				span.SetAttributes(attribute.Int("http.response.status_code", status))
				// 只把 5xx 标成错误。4xx 是调用方的问题（库存不足、未登录），
				// 标红会让「错误率」这个信号失去意义。
				if status >= 500 {
					span.SetStatus(codes.Error, http.StatusText(status))
				}
				span.End()
			}

			obs.HTTPRequests.WithLabelValues(route, r2.Method, strconv.Itoa(status)).Inc()
			obs.HTTPDuration.WithLabelValues(route, r2.Method).Observe(elapsed.Seconds())

			if r2.URL.Path == "/metrics" || r2.URL.Path == "/healthz" || r2.URL.Path == "/readyz" {
				return
			}
			reqLog.Info("http_request",
				"method", r2.Method,
				"path", r2.URL.Path,
				"route", route,
				"status", status,
				"dur_ms", elapsed.Milliseconds(),
				"bytes", sw.bytes,
				"remote", r2.RemoteAddr,
			)
		}()

		next.ServeHTTP(sw, r2)
	})
}
