#!/usr/bin/env bash
# git push 分层诊断：一次跑完，看到底是 DNS / 代理 / 认证 / 远端哪一层坏了。
# 用法：scripts/git-doctor.sh [remote]
set -u

REMOTE="${1:-origin}"
PROXY="${HTTPS_PROXY:-${https_proxy:-}}"
FAIL=0

ok()   { printf '  \033[32mOK\033[0m   %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAIL=1; }
warn() { printf '  \033[33mWARN\033[0m %s\n' "$1"; }

echo "== 1. DNS =="
for h in github.com api.github.com ssh.github.com; do
  ips=$(dscacheutil -q host -a name "$h" 2>/dev/null | awk '/ip_address/{printf "%s ", $2}')
  if [ -z "$ips" ]; then
    bad "$h 解析不到"
  else
    ok "$h -> $ips"
  fi
done

echo "== 2. 代理到 github.com 的 CONNECT 隧道 =="
if [ -z "$PROXY" ]; then
  warn "没有 https_proxy，直连测试"
  code=$(curl -s -o /dev/null -m 10 -w '%{http_code}' "https://github.com/Jeffrey-Xu/shop-gitops.git/info/refs?service=git-upload-pack" 2>/dev/null)
else
  echo "  代理: $PROXY"
  code=$(curl -s -o /dev/null -m 10 -w '%{http_code}' -x "$PROXY" "https://github.com/Jeffrey-Xu/shop-gitops.git/info/refs?service=git-upload-pack" 2>/dev/null)
fi
case "$code" in
  200) ok "git-upload-pack 200" ;;
  000) bad "连不上（超时 / CONNECT 被拒 / 502）—— 这一层就是 502 的来源" ;;
  *)   warn "返回 $code" ;;
esac

echo "== 3. api.github.com（API 兜底通道）=="
code=$(curl -s -o /dev/null -m 10 -w '%{http_code}' https://api.github.com/rate_limit 2>/dev/null)
[ "$code" = "200" ] && ok "api 200" || bad "api 返回 $code"

echo "== 4. gh 认证 =="
if gh auth status >/dev/null 2>&1; then
  ok "$(gh auth status 2>&1 | grep -o 'account [A-Za-z0-9_-]*' | head -1)"
else
  bad "gh 未登录或 token 失效（gh auth status 看详情）"
fi

echo "== 5. git 远端协商 =="
if out=$(timeout 20 git ls-remote "$REMOTE" HEAD 2>&1); then
  ok "ls-remote 成功：$(echo "$out" | awk '{print substr($1,1,8)}')"
else
  bad "ls-remote 失败：$(echo "$out" | tail -2 | tr '\n' ' ')"
fi

echo
if [ "$FAIL" -eq 0 ]; then
  echo "结论：链路正常，git push 可以直接用。"
else
  echo "结论：至少一层失败。第 2 步失败 → 走 scripts/push-via-api.py 兜底；"
  echo "      第 4 步失败 → gh auth login；第 1 步失败 → 换网络或写 /etc/hosts。"
fi
exit "$FAIL"
