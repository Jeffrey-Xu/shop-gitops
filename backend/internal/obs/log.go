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

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	log := slog.New(h).With("service", service, "version", version)
	slog.SetDefault(log)
	return log
}
