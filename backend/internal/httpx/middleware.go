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

		r2 := r.WithContext(context.WithValue(r.Context(), ridKey{}, rid))
		// 必须用局部变量。直接 log = log.With(...) 会在闭包里累积，
		// 每个请求再加一层 request_id，并发时还会互相踩（data race）。
		reqLog := log.With("request_id", rid)

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
