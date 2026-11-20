#!/usr/bin/env bash
# 本地构建前端镜像（Docker Hub 拉不到 nginx 时的绕行方案）
#
# 正常路径是 frontend/Dockerfile 的两阶段构建：node 里 vite build，
# 再 COPY 进 nginx 镜像。但 Docker Hub 的 nginx:1.27-alpine 经常 502/超时，
# 这个脚本改成「复用本地已有的前端镜像，只替换静态产物」：
#
#   1. 只跑 Dockerfile 的 build 阶段（node:22-alpine 本地已有）
#   2. 把 /app/dist 从中间镜像里倒出来
#   3. 以现有前端镜像为基础，覆盖 /usr/share/nginx/html
#
# 用法: scripts/build-frontend-local.sh [tag]
set -euo pipefail

TAG="${1:-local}"
REGISTRY="ghcr.io/jeffrey-xu/shop-gitops"
BASE_IMAGE="${REGISTRY}/frontend:fulfill1"
# 找不到 base 时退回到任意一个已有的前端镜像
if ! docker image inspect "$BASE_IMAGE" >/dev/null 2>&1; then
  BASE_IMAGE="$(docker images --format '{{.Repository}}:{{.Tag}}' \
    | grep "${REGISTRY}/frontend:" | head -1)"
fi

if [ -z "${BASE_IMAGE:-}" ]; then
  echo "没有可用的前端基础镜像，先手动 docker pull nginx:1.27-alpine 再走 Dockerfile" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "==> 1/3 构建静态产物"
docker build --target build --build-arg BUILD_ID="$TAG" \
  -t shop-frontend-dist:tmp "$ROOT/frontend" >/dev/null

echo "==> 2/3 导出 dist"
docker run --rm shop-frontend-dist:tmp tar -cf - -C /app dist | tar -xf - -C "$WORK"

echo "==> 3/3 基于 $BASE_IMAGE 打包"
cp "$ROOT/frontend/nginx.conf" "$WORK/"
cat > "$WORK/Dockerfile.local" <<EOF
FROM ${BASE_IMAGE}
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY dist /usr/share/nginx/html
EOF
docker build -f "$WORK/Dockerfile.local" -t "${REGISTRY}/frontend:${TAG}" "$WORK"

echo "完成: ${REGISTRY}/frontend:${TAG}"
