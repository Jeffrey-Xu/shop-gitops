# 构建镜像必须跟 go.mod 的 go 指令对齐：golang 官方镜像默认 GOTOOLCHAIN=local，
# 版本低了不会自动切工具链，而是直接报 "go.mod requires go >= 1.26.0"。
# 这次是被 golang.org/x/crypto v0.57 抬上去的（钉回 1.24 要连带降 x/crypto、
# grpc-gateway、genproto 一串，不如直接把构建镜像跟上）。
FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# 一个镜像三个二进制：api / worker / account 共享 internal 包，
# 部署时靠 command 区分角色，CI 只构建一次，版本天然一致。
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/account ./cmd/account

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 app
COPY --from=build /out/api /app/api
COPY --from=build /out/worker /app/worker
COPY --from=build /out/account /app/account
USER app
EXPOSE 8080
CMD ["/app/api"]
