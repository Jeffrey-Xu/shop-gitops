// replay 把死信队列里的消息重投回正常链路。
//
// 为什么需要它：DLQ 只解决「不丢」，不解决「怎么捞回来」。
// 没有这个工具时，一次持续三分钟的支付故障会在 DLQ 里留下几十条消息，
// 而其中一部分的订单钱已经扣了、状态停在 CREATED ——
// 有人盯着看才会发现，没人看就永远躺在那儿。
//
// 默认 dry-run：读一遍、打印、原样放回，队列状态和读之前完全一样。
// 加 -apply 才真正重投。
//
// 重投之后会发生什么：消息带着清空的 attempts 回到主队列，消费端照常跑
// 幂等闸门（订单状态、支付回查），所以重复重放是安全的。
// 但如果订单已经被 cancel（重试耗尽时补偿过），worker 会直接短路 ——
// 那条消息不会「复活」这张单，需要人工决定是否重新下单。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/broker"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
)

// flow 描述一条链路的死信怎么捞回来。
//
// RestoreRoute 只用于 dry-run：它绑在 DLX 上，能把消息原样送回 DLQ。
// 少了它，dry-run 就只能在「删掉消息」和「什么都不打印」之间选。
type flow struct {
	alias        string
	source       string
	restoreRoute string
	targetRoute  string
}

var flows = []flow{
	{
		alias:        "order.created",
		source:       broker.QueueOrderDLQ,
		restoreRoute: broker.RouteOrderDLQ,
		targetRoute:  broker.RouteOrderCreated,
	},
	{
		alias:        "order.paid",
		source:       broker.QueueOrderPaidDLQ,
		restoreRoute: broker.RouteOrderPaidDLQ,
		targetRoute:  broker.RouteOrderPaid,
	},
}

func main() {
	apply := flag.Bool("apply", false, "真正重投（默认只 dry-run 打印）")
	queue := flag.String("queue", "all", "order.created | order.paid | all")
	limit := flag.Int("limit", 0, "每个队列最多处理多少条，0 表示用默认上限 1000")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("replay")
	if err != nil {
		log.Error("config_load_failed", "err", err)
		os.Exit(1)
	}

	selected, err := pickFlows(*queue)
	if err != nil {
		log.Error("bad_queue_flag", "err", err)
		os.Exit(1)
	}

	brk, err := broker.Connect(ctx, cfg.RabbitURL, log)
	if err != nil {
		log.Error("broker_connect_failed", "err", err)
		os.Exit(1)
	}
	defer brk.Close()

	if !*apply {
		fmt.Println("dry-run：只读不投。确认无误后加 -apply 执行。")
		fmt.Println()
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	totalMatched, totalReplayed := 0, 0

	for _, f := range selected {
		depth, derr := brk.QueueDepth(f.source)
		if derr != nil {
			log.Error("queue_inspect_failed", "queue", f.source, "err", derr)
			os.Exit(1)
		}
		fmt.Printf("== %s（%s）当前 %d 条\n", f.alias, f.source, depth)
		if depth == 0 {
			fmt.Println("   空，跳过")
			fmt.Println()
			continue
		}

		fmt.Fprintln(tw, "  订单\t尝试\t重放次数\t上次错误\t字节")
		st, rerr := brk.Replay(ctx, broker.ReplayOptions{
			Source:         f.source,
			RestoreRoute:   f.restoreRoute,
			TargetExchange: broker.Exchange,
			TargetRoute:    f.targetRoute,
			Limit:          *limit,
			DryRun:         !*apply,
			OnMessage: func(headers amqp.Table, body []byte) error {
				printRow(tw, headers, body)
				return nil
			},
		})
		tw.Flush()
		if rerr != nil {
			log.Error("replay_failed", "queue", f.source, "matched", st.Matched, "err", rerr)
			os.Exit(1)
		}
		totalMatched += st.Matched
		totalReplayed += st.Replayed
		fmt.Printf("   读到 %d 条，重投 %d 条\n\n", st.Matched, st.Replayed)
	}

	if *apply {
		fmt.Printf("完成：读到 %d 条，重投 %d 条。\n", totalMatched, totalReplayed)
		fmt.Println("下一步：看 worker 日志里 order_retry_scheduled / payment_captured，确认消息被消费；")
		fmt.Println("       再查 orders 表，确认这些单不再停在 CREATED。")
	} else {
		fmt.Printf("dry-run 结束：读到 %d 条，未改动任何消息。\n", totalMatched)
		fmt.Println("确认后用同样的命令加 -apply 执行。")
	}
}

func pickFlows(name string) ([]flow, error) {
	if name == "" || name == "all" {
		return flows, nil
	}
	for _, f := range flows {
		if f.alias == name {
			return []flow{f}, nil
		}
	}
	return nil, fmt.Errorf("未知队列 %q，可选 order.created / order.paid / all", name)
}

// printRow 只打最要紧的四列：谁的单、试了几次、上次为什么失败、消息多大。
// 不打印整个 body —— 一个订单的 items 可能有几十行，刷屏之后没人看得下去。
func printRow(w *tabwriter.Writer, headers amqp.Table, body []byte) {
	var m map[string]any
	_ = json.Unmarshal(body, &m) // 解不开也要能打印出来，body 坏了本身就是信息

	orderID := "-"
	if v, ok := m["orderId"]; ok {
		orderID = fmt.Sprint(v)
	}
	attempts := "-"
	if v, ok := headers["attempts"]; ok {
		attempts = fmt.Sprint(v)
	}
	replayed := "0"
	if v, ok := headers["replay_count"]; ok {
		replayed = fmt.Sprint(v)
	}
	lastErr := "-"
	if v, ok := headers["last_error"].(string); ok {
		lastErr = truncate(v, 48)
	}
	fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%d\n", orderID, attempts, replayed, lastErr, len(body))
}

func truncate(s string, n int) string {
	// 按 rune 截断，不要按字节 —— 中文错误信息按字节切会切出半个字
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
