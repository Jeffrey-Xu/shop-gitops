package store

import (
	"strings"
	"testing"

	"github.com/XSAM/otelsql"
)

// splitStatements 是迁移的唯一执行器：pgx 走扩展协议，一条 Exec 里
// 塞多条语句会直接报错，所以切错一个分号就等于一个迁移整体失败。
func TestSplitStatements(t *testing.T) {
	body := `-- 文件头注释
-- 第二行注释

CREATE TABLE a (
    id int,
    name text
);

INSERT INTO a VALUES (1, 'x');

-- 中间注释
ALTER TABLE a ADD COLUMN flag boolean;

SELECT 1;
`
	got := splitStatements(body)

	if len(got) != 4 {
		t.Fatalf("切出 %d 条语句，期望 4 条：%#v", len(got), got)
	}
	for i, stmt := range got {
		if strings.Contains(stmt, "--") {
			t.Fatalf("第 %d 条里还留着注释：%q", i+1, stmt)
		}
		if !strings.HasSuffix(strings.TrimSpace(stmt), ";") {
			t.Fatalf("第 %d 条没有以分号结尾：%q", i+1, stmt)
		}
	}
	// 多行语句不能被拆开：CREATE TABLE 的括号里没有分号，
	// 但它跨了 4 行 —— 只看「行尾有没有分号」是对的，
	// 按行切就是错的。
	if !strings.Contains(got[0], "\n") {
		t.Fatalf("多行语句被压成了一行：%q", got[0])
	}
}

// 最后一条没有分号也不能被丢掉：那是「写漏了分号」，
// 应该让数据库报语法错误，而不是静默少跑一段。
func TestSplitStatementsKeepsTrailing(t *testing.T) {
	got := splitStatements("SELECT 1;\nSELECT 2")
	if len(got) != 2 {
		t.Fatalf("切出 %d 条，期望 2 条：%#v", len(got), got)
	}
}

func TestSplitStatementsEmpty(t *testing.T) {
	if got := splitStatements("-- 只有注释\n\n"); len(got) != 0 {
		t.Fatalf("只有注释时应返回空，实际 %#v", got)
	}
}

// dbSpanName 决定 Tempo 里 SQL span 的名字。名字不能带参数值，
// 否则基数会随数据增长（WHERE id = 1 / 2 / 3…），按名字聚合直接失效。
func TestDBSpanName(t *testing.T) {
	cases := []struct {
		method otelsql.Method
		query  string
		want   string
	}{
		{otelsql.Method("BEGIN"), "", "BEGIN"},
		{otelsql.Method("Query"), "SELECT id FROM orders WHERE id = $1", "SELECT orders"},
		{otelsql.Method("Exec"), "INSERT INTO outbox (topic, payload) VALUES ($1, $2)", "INSERT outbox"},
		{otelsql.Method("Exec"), "  update orders set status = 'PAID'", "UPDATE orders"},
		{otelsql.Method("Exec"), "DELETE FROM sessions", "DELETE sessions"},
	}
	for _, tc := range cases {
		if got := dbSpanName(nil, tc.method, tc.query); got != tc.want {
			t.Fatalf("dbSpanName(%q) = %q，期望 %q", tc.query, got, tc.want)
		}
	}
}
