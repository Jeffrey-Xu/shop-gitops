// Package obs 是可观测性的地基：结构化日志 + Prometheus 指标。
package obs

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger 输出 JSON 到 stdout。
// 为什么必须是 JSON：Alloy 只会做 stage.cri 拆行，纯文本里没法可靠提取
// level / request_id / order_id，Loki 里就只能全文搜索。
func NewLogger(level, service, version string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: lvl,
		// slog 的 Level.String() 返回 "INFO"/"WARN" 大写，而 pino 和 Python
		// 的 logging 都是小写。同一个 Loki 里两种大小写并存，
		// 按 {level="error"} 过滤就会漏掉 Go 的 ERROR —— 这种漏在下一次故障里才会发现。
		// 统一在产生日志的这一端解决，比在采集端加转换更可靠
		// （Alloy 的 template stage 还会和 Helm 的 {{ }} 打架）。
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// 只处理顶层：业务字段里如果有同名字段不该被改
			if len(groups) == 0 && a.Key == slog.LevelKey {
				if l, ok := a.Value.Any().(slog.Level); ok {
					a.Value = slog.StringValue(strings.ToLower(l.String()))
				}
			}
			return a
		},
	})
	log := slog.New(h).With("service", service, "version", version)
	slog.SetDefault(log)
	return log
}
