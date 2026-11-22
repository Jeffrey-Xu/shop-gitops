// Package obs 是可观测性的地基：结构化日志 + Prometheus 指标 + 分布式链路。
//
// 三者的 service 名和版本必须来自同一个地方，否则 Grafana 里按服务名过滤时
// 指标和 trace 会对不上——这种不一致排查起来比没数据还费劲。
package obs

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/Jeffrey-Xu/shop-gitops/backend"

// Tracer 每次调用都向全局 provider 取一次。
// 不要在包级变量里缓存：InitTracing 之前取到的会是 noop，
// 之后所有 span 就静默消失了（这个坑不报错，只是图上什么都不显示）。
func Tracer() trace.Tracer {
	return otel.Tracer(instrumentationName)
}

// InitTracing 装配 OTLP 导出器。返回的 shutdown 必须在进程退出前调用，
// 否则 batch 里还没发出去的 span 会丢——正好丢掉最关键的崩溃现场。
//
// OTEL_EXPORTER_OTLP_ENDPOINT 没设时返回一个空 shutdown 并保持 noop：
// 本地 go run 不起 Tempo 也能跑，不会刷一屏导出失败。
func InitTracing(ctx context.Context, service, version string) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		slog.Info("tracing_disabled", "reason", "OTEL_EXPORTER_OTLP_ENDPOINT 未设置")
		return func(context.Context) error { return nil }, nil
	}

	// WithEndpoint 只吃 host:port，协议和路径由 exporter 自己拼（/v1/traces）。
	// 所以这里得把 scheme 和尾部斜杠剥掉，否则会拼成 http://http://tempo/v1/traces
	host := strings.TrimSuffix(endpoint, "/")
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")

	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(host),
		otlptracehttp.WithInsecure(), // 集群内网，TLS 由 NetworkPolicy/mesh 那层解决
		otlptracehttp.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, err
	}

	pod, _ := os.Hostname()
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", service),
		attribute.String("service.version", version),
		attribute.String("k8s.pod.name", pod),
		attribute.String("deployment.environment.name", envOr("APP_ENV", "local")),
	))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		// 2s 一批而不是默认的 5s：演示和排障都要「下完单立刻能在 Tempo 里看到」，
		// 等 5 秒会让人以为链路没接通。
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio()))),
	)
	otel.SetTracerProvider(tp)
	// W3C TraceContext 是跨语言的最小公约数：Go、Node、Python 三套 SDK
	// 只在这个格式上能互相对上。Baggage 一起配上，将来传业务上下文不用改代码。
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	// 导出失败默认是静默的（只在 stderr 打一行）。接进 slog 才能被 Alloy 收走，
	// 否则 Tempo 里少了 span 而日志里毫无线索。
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		slog.Warn("otel_error", "err", err)
	}))

	slog.Info("tracing_enabled", "endpoint", host, "service", service, "ratio", sampleRatio())
	return tp.Shutdown, nil
}

// sampleRatio 允许用 OTEL_TRACES_SAMPLER_ARG 调采样率，默认全采。
// 本地量小，且演示时不能出现「这单刚好没被采到」——
// 解析失败一律退回 1.0，宁可多存也不要在排障时发现链路断在半路。
func sampleRatio() float64 {
	v := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG"))
	if v == "" {
		return 1.0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 || f > 1 {
		return 1.0
	}
	return f
}

// Traceparent 把当前 span 压成 W3C 头字符串，用于落库或塞进非 HTTP 的消息头。
func Traceparent(ctx context.Context) string {
	c := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, c)
	return c.Get("traceparent")
}

// ContextFromTraceparent 是 Traceparent 的逆操作：从库里读回来的字符串
// 还原成一个「远程父 span」的 context。
//
// 空字符串返回 Background：老数据（迁移前写的 outbox 行）没有 traceparent，
// 这时候 relay 自己开一条新 trace，而不是报错卡住投递。
func ContextFromTraceparent(tp string) context.Context {
	if tp == "" {
		return context.Background()
	}
	return otel.GetTextMapPropagator().Extract(
		context.Background(),
		propagation.MapCarrier{"traceparent": tp},
	)
}

// TraceID 给日志用。返回空串表示当前 ctx 没在 span 里，
// 调用方应据此决定要不要写这个字段（空值比缺字段更难在 Loki 里过滤）。
func TraceID(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return sc.TraceID().String()
	}
	return ""
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
