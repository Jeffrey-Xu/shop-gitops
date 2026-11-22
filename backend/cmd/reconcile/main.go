// reconcile 对账：找出「钱的事实」和「订单的事实」对不上的那些单，能自动收敛的就收敛。
//
// 为什么必须有这个工具：在线链路只覆盖「跑到一半没挂」的情况。
// worker 扣款成功之后进程被杀、DB 抖动、Pod 被驱逐，都会留下一种单：
// 网关那边钱已经落了账，而我们这边订单还停在 CREATED。
// 这种单不会自己好，也不会有人来投诉 —— 用户只知道「付了钱没收到货」，
// 然后不再下单。上一版更糟：重试耗尽时把它 cancel 掉，
// 变成「钱收了、单作废、还不退款」。
//
// 四类差异、四种处置，绝不能混为一谈（详见 store.MismatchKind）：
//
//	stuck_paid          已扣款、订单还 CREATED          → 前推到 PAID（幂等）
//	paid_but_cancelled  已扣款、订单已 CANCELLED        → 退款
//	amount_mismatch     扣款金额 ≠ 订单金额             → 只报告，人看
//	paid_but_declined   订单 PAID、网关 failed          → 只报告，人看
//
// 默认 dry-run 只读。加 -apply 才会前推；退款还额外要求 -refund，
// 因为前推是「把已收的钱兑现成货」，退款是「动钱」——
// 两个动作的风险不对等，闸门不该一样宽。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/config"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/payment"
	"github.com/Jeffrey-Xu/shop-gitops/backend/internal/store"
)

// kindMeta 把「机器分类」翻译成「人该怎么处置」。
// 工具的输出要能直接当操作手册看：看到哪一类，就知道下一步做什么。
type kindMeta struct {
	kind   store.MismatchKind
	alias  string
	impact string
	action string
	auto   bool // 是否允许 -apply 自动处置
}

var kinds = []kindMeta{
	{
		kind:   store.KindStuckPaid,
		alias:  "stuck-paid",
		impact: "钱已收、货不发",
		action: "前推到 PAID（可自动）",
		auto:   true,
	},
	{
		kind:   store.KindPaidButCancelled,
		alias:  "paid-but-cancelled",
		impact: "钱已收、单作废",
		action: "全额退款（需 -refund）",
		auto:   true,
	},
	{
		kind:   store.KindAmountMismatch,
		alias:  "amount-mismatch",
		impact: "金额对不上",
		action: "只报告，人工核实",
		auto:   false,
	},
	{
		kind:   store.KindPaidButDeclined,
		alias:  "paid-but-declined",
		impact: "没收到钱却要发货",
		action: "只报告，人工核实",
		auto:   false,
	},
}

func main() {
	apply := flag.Bool("apply", false, "真正处置（默认只 dry-run 打印）")
	refund := flag.Bool("refund", false, "允许退款。没有它时 paid_but_cancelled 只列出来，不动钱")
	minAge := flag.Duration("min-age", store.DefaultMismatchGrace, "订单最后更新多久之后才算不一致")
	limit := flag.Int("limit", 200, "最多列出多少条")
	kindFlag := flag.String("kind", "all", "stuck-paid | paid-but-cancelled | amount-mismatch | paid-but-declined | all")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("reconcile")
	if err != nil {
		log.Error("config_load_failed", "err", err)
		os.Exit(1)
	}

	st, err := store.Open(ctx, cfg.DatabaseURL, log, cfg.DBMaxOpen, cfg.DBMaxIdle, cfg.DBConnMaxLifetime)
	if err != nil {
		log.Error("database_open_failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	selected, err := pickKinds(*kindFlag)
	if err != nil {
		log.Error("bad_kind_flag", "err", err)
		os.Exit(1)
	}

	items, err := st.ScanMismatches(ctx, *minAge, *limit)
	if errors.Is(err, store.ErrPaymentsTableMissing) {
		fmt.Println("没有 payments 表 —— 说明 payment-service 从没跑过（本地模拟支付模式）。")
		fmt.Println("这种部署下「钱」只存在于 orders 状态里，没有可对的东西。")
		return
	}
	if err != nil {
		log.Error("scan_failed", "err", err)
		os.Exit(1)
	}

	if !*apply {
		fmt.Printf("dry-run：只读不处置。放宽限期 %s，扫到 %d 条不一致。\n", minAge.String(), len(items))
		fmt.Println()
	}

	pay := payment.New(cfg.PaymentURL, cfg.PaymentTimeout, log)
	if *apply && !pay.Enabled() {
		fmt.Println("注意：未配置 PAYMENT_URL，退款无法执行（前推不受影响）。")
		fmt.Println()
	}

	var forwarded, refunded, skipped, failed int
	for _, km := range selected {
		batch := filter(items, km.kind)
		printHeader(km, len(batch))
		if len(batch) == 0 {
			continue
		}

		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  订单\t订单状态\t支付状态\t订单金额\t扣款金额\t支付单号\t已退款\t年龄")
		for _, m := range batch {
			fmt.Fprintf(tw, "  %d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				m.OrderID, m.OrderStatus, m.PaymentStatus,
				money(m.OrderCents), money(m.PaymentCents),
				orDash(m.PaymentID), yesNo(m.Refunded), age(m.AgeSeconds))
		}
		tw.Flush()
		fmt.Println()

		if !*apply || !km.auto {
			continue
		}

		for _, m := range batch {
			switch km.kind {
			case store.KindStuckPaid:
				changed, err := st.MarkOrderPaid(ctx, m.OrderID,
					"reconcile: payment captured ("+m.PaymentID+")", m.PaymentID)
				switch {
				case err != nil:
					failed++
					log.Error("forward_failed", "order_id", m.OrderID, "err", err)
				case !changed:
					// 两次扫描之间被别人处理了（replay、worker 自己好了）。
					// 这不是失败，是收敛。
					skipped++
					log.Info("forward_skipped_already_finalized", "order_id", m.OrderID)
				default:
					forwarded++
					log.Info("forwarded", "order_id", m.OrderID, "payment_id", m.PaymentID)
				}

			case store.KindPaidButCancelled:
				if m.Refunded {
					skipped++
					continue
				}
				if !*refund {
					skipped++
					log.Warn("refund_not_authorized", "order_id", m.OrderID, "hint", "加 -refund 才会真的退钱")
					continue
				}
				res, err := pay.Refund(ctx, strconv.FormatInt(m.OrderID, 10),
					"reconcile: order cancelled but payment captured")
				switch {
				case errors.Is(err, payment.ErrPaymentNotFound):
					// 网关那边压根没这笔（多半是模拟支付的单），不需要退。
					skipped++
					log.Warn("refund_no_payment_at_gateway", "order_id", m.OrderID)
				case errors.Is(err, payment.ErrNotRefundable):
					failed++
					log.Error("refund_not_refundable", "order_id", m.OrderID)
				case err != nil:
					failed++
					log.Error("refund_failed", "order_id", m.OrderID, "err", err)
				default:
					refunded++
					log.Info("refunded", "order_id", m.OrderID,
						"refund_id", res.RefundID, "amount_cents", res.AmountCents, "replayed", res.Replayed)
				}
			}
		}
	}

	printSummary(*apply, *refund, forwarded, refunded, skipped, failed, len(items))
	if failed > 0 {
		os.Exit(1)
	}
}

func pickKinds(name string) ([]kindMeta, error) {
	if name == "" || name == "all" {
		return kinds, nil
	}
	for _, k := range kinds {
		if k.alias == name {
			return []kindMeta{k}, nil
		}
	}
	aliases := make([]string, 0, len(kinds))
	for _, k := range kinds {
		aliases = append(aliases, k.alias)
	}
	return nil, fmt.Errorf("未知类型 %q，可选 %s / all", name, strings.Join(aliases, " | "))
}

func filter(items []store.Mismatch, kind store.MismatchKind) []store.Mismatch {
	out := make([]store.Mismatch, 0, len(items))
	for _, m := range items {
		if m.Kind == kind {
			out = append(out, m)
		}
	}
	return out
}

func printHeader(km kindMeta, n int) {
	fmt.Printf("== %s（%s）%d 条 —— %s\n", km.alias, km.impact, n, km.action)
}

func printSummary(applied, refundAllowed bool, forwarded, refunded, skipped, failed, total int) {
	fmt.Println("────────────────────────────────────────────")
	switch {
	case !applied:
		fmt.Printf("dry-run 结束：共 %d 条不一致，未改动任何数据。\n", total)
		fmt.Println("确认后用同样的命令加 -apply；要退款再加 -refund。")
	case !refundAllowed:
		fmt.Printf("完成：前推 %d，退款 %d（未授权），跳过 %d，失败 %d。\n", forwarded, refunded, skipped, failed)
		fmt.Println("若要处理「钱已收、单作废」，加 -refund 重跑一次。")
	default:
		fmt.Printf("完成：前推 %d，退款 %d，跳过 %d，失败 %d。\n", forwarded, refunded, skipped, failed)
	}
	fmt.Println("对账不会「一次清空」：新差异随时会产生，看 shop_payment_mismatch 指标是否长期为 0。")
	if failed > 0 {
		fmt.Println("有失败项，退出码非 0 —— 上面的日志逐条说明了原因。")
	}
}

func money(cents int64) string {
	return fmt.Sprintf("¥%.2f", float64(cents)/100)
}

func age(seconds float64) string {
	d := time.Duration(seconds * float64(time.Second))
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func yesNo(b bool) string {
	if b {
		return "是"
	}
	return "否"
}
