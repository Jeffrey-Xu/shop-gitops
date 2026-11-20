# shop-gitops

一个刻意做小的电商后端，用来把 **缓存 + 消息队列 + 事务性 outbox + 可观测性** 这几件事在真集群里跑通。

前端是静态站，后端是 Go（`api` + `worker` 两个入口同一个镜像），数据落宿主机的 PostgreSQL，Redis 做读缓存，RabbitMQ 做异步事件。另外有一个独立的 **Python/FastAPI 支付服务**（`payment-service/`），用同一个 PG 的不同表。交付走 **GitHub Actions 构建 + ArgoCD 同步**。

```
改代码 push
   │
   ├─ GitHub Actions  ── build backend/frontend（amd64+arm64）→ push ghcr.io
   │                     └─ 回写 deploy/kustomization.yaml 的 image tag 并提交
   │
   └─ ArgoCD          ── 发现新 commit → 对比 live state → 自动 sync
```

## 目录

| 路径 | 内容 |
|---|---|
| `backend/cmd/api/` | HTTP 服务入口，另起一个 outbox relay 协程 |
| `backend/cmd/worker/` | RabbitMQ 消费者入口，同时提供 `/healthz` 与 `/metrics` |
| `backend/internal/` | `config` `obs` `httpx` `cache` `store` `broker` `outbox` `api` `worker` |
| `backend/internal/store/migrations/` | `go:embed` 的 SQL 迁移，启动时同步执行，失败即退出 |
| `frontend/` | Vite + 原生 JS，构建成 `dist/` 由 nginx 托管 |
| `payment-service/` | Python 3.12 + FastAPI，模拟第三方支付网关（幂等扣款 + 落库 + 回查） |
| `deploy/` | Kustomize：namespace、api/worker/frontend/payment、ingress、monitoring |
| `argocd/` | ArgoCD 引导清单（`install.yaml` 不进 Git） |
| `monitoring/` | metrics-server / kube-prometheus-stack / loki / alloy 的 Application |
| `monitoring/extras/` | mailpit、宿主 PG/Redis exporter、Grafana 业务大盘（由 `monitoring-extras` 这个 Application 管） |
| `scripts/` | `git-doctor.sh` 推送链路诊断、`push-via-api.py` 兜底提交 |
| `.github/workflows/ci.yaml` | 矩阵构建 + 回写 tag |

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/hello` | 返回 Pod 名与版本，验证分流 |
| GET | `/api/products` | 商品列表，走 Redis，响应头带 `X-Cache: HIT\|MISS` |
| GET | `/api/products/{id}` | 单商品 |
| POST | `/api/orders` | 下单，扣库存 + 建单 + 写 outbox，一个事务 |
| GET | `/api/orders/{id}` | 订单详情含状态迁移历史 |
| GET | `/api/stats` | 缓存命中率、队列深度、各状态订单数，前端 footer 用 |
| POST | `/api/dev/fault?type=panic\|slow\|error` | 故障注入，仅 `DEV_MODE=true` 时存在 |
| GET | `/metrics` | Prometheus 指标（api 与 worker 各一份） |
| GET | `/healthz` | liveness，不查任何依赖 |
| GET | `/readyz` | readiness，查 PG **和** Redis，任一失败即 503 |

> `healthz` 与 `readyz` 分开：依赖挂了应该摘流量，不该被 liveness 反复重启。
> 注意 `readyz` 目前把 Redis 当成硬依赖，Redis 挂会导致整个 api 摘流量——见「已知限制」。

支付服务（独立进程，集群内 `svc/payment:8080`，集群外走 `/pay` 前缀，ingress 用正则重写）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/charge` | `{order_id, amount_cents, currency}` → 扣款；同一 `order_id` 重放返回同一条记录且 `replayed=true` |
| GET | `/payments/{order_id}` | 回查扣款结果，不存在返回 404 |
| GET | `/healthz` `/readyz` `/metrics` | 存活 / 就绪（查 PG）/ 指标 |

```bash
curl -X POST http://localhost/pay/charge -H 'Content-Type: application/json' \
  -d '{"order_id":"ord_001","amount_cents":9900}' -i | tail -1
# 再发一次 → 同一个 payment_id，replayed: true（幂等，不会重复扣款）
```

## 一次下单的完整路径

```
POST /api/orders  (Idempotency-Key 可选)
   │
   ├─ httpx.Wrap            生成/透传 X-Request-ID，记指标与访问日志
   │
   ├─ 单事务 ─────────────────────────────────────────────┐
   │    UPDATE products SET stock = stock - $1            │
   │     WHERE id = $2 AND stock >= $1 RETURNING price    │ ← 条件更新即乐观锁
   │    INSERT orders  (status='CREATED', idempotency_key)│ ← 撞唯一键则回滚并返回旧单
   │    INSERT order_items (快照单价)                      │
   │    INSERT order_events ('' → 'CREATED')              │
   │    INSERT outbox (topic='order.created')             │ ← 和订单同一事务
   │  COMMIT                                              │
   │                                                      │
   └─ 提交后：DEL 商品缓存（失败只 warn，不回滚）──────────┘
   │
   ▼
outbox relay（api 进程内，每 2s 扫表）
   │  Publish + publisher confirm  →  published_at = now()
   ▼
shop.events (topic) ──order.created──▶ shop.order.created
   │
   ▼
worker：basic.consume（push，非轮询，prefetch=5）
   ├─ 闸门 1：status != CREATED → ack 跳过（重复投递短路）
   ├─ POST http://payment:8080/charge   ← 真实扣款，3s 超时，按 order_id 幂等
   ├─ 闸门 2：UPDATE ... WHERE status='CREATED'（乐观锁）
   │           + order_events 唯一约束兜底
   └─ ack → 订单变 PAID（order_events.note 里带 payment_id）
```

失败路径（worker 内），**关键是把"拒付"和"临时故障"分开**：

```
POST /charge 的三种结局
 ├─ succeeded              ──▶ 闸门 2 → PAID
 ├─ failed / 4xx（拒付）   ──▶ CancelOrder（CANCELLED + 库存回补）──▶ ack   [不重试]
 │                              重试一百次也是同一张卡被拒，纯浪费
 └─ 超时 / 5xx / 连不上
      ├─ attempts < 3 ──▶ publish 到 shop.dlx / .retry（TTL 10s 回主队列）──▶ ack
      └─ attempts >= 3 ─▶ publish 到 .dlq ──▶ CancelOrder ──▶ ack
```

未配置 `PAYMENT_URL` 时 worker 退回内置模拟支付，本地不想起 Python 服务也能跑通全链路。

设计取舍：**at-least-once + 消费端幂等**，而不是「commit 完再 publish」。后者会丢消息且无法发现。代价是可能重复投递，由两道闸门消化。

## 数据模型

| 表 | 关键字段 | 说明 |
|---|---|---|
| `products` | `price_cents INT`、`stock`、`version` | 金额用整数分，不用浮点 |
| `orders` | `status`、`total_cents`、`idempotency_key UNIQUE` | 状态机 `CREATED → PAID / CANCELLED` |
| `order_items` | `unit_price_cents` | 下单时快照单价，改价不影响历史单 |
| `order_events` | `from_status`、`to_status`、`UNIQUE(order_id,to_status)` | 状态迁移审计，日志之外的第二证据 |
| `outbox` | `topic`、`payload jsonb`、`published_at`、`attempts` | 未投递事件队列 |

**库存只在 PostgreSQL 扣，Redis 不参与扣减。** 缓存只负责读得快，一致性交给数据库。

## 部署形态

集群里一个 `shop` 命名空间，Kustomize 渲染出 17 个对象：

```
Namespace shop
ConfigMap shop-config        APP_VERSION / LOG_LEVEL / CACHE_TTL / DEV_MODE
                             REDIS_ADDR / MQ_PREFETCH / MQ_MAX_ATTEMPTS
                             PAYMENT_URL / PAYMENT_TIMEOUT
Secret    shop-secret        database-url / rabbitmq-url
Deployment api     1 副本    /app/api      req 50m/64Mi   limit 500m/256Mi
Deployment worker  1 副本    /app/worker   req 30m/48Mi   limit 300m/192Mi
Deployment frontend 1 副本   nginx:1.27    req 20m/32Mi   limit 200m/128Mi
Deployment payment 1 副本    uvicorn       req 50m/96Mi   limit 300m/256Mi
Service   api / frontend / worker / payment
Ingress   shop               shop.localhost + 无 host 兜底
          payment            /pay(/|$)(.*) → rewrite /$2
ServiceMonitor api / worker / payment
PrometheusRule shop         3 条告警
```

- 三个 Deployment 都是 `replicas: 1` + `strategy: Recreate`。本地学习环境，不做高可用
- 容器以 uid `10001` 非 root 运行，`readOnlyRootFilesystem`，drop 所有 capability
- 探针：readiness `/readyz`（5s/5s，失败 3 次摘流量）、liveness `/healthz`（20s/10s）
- Ingress 带 `limit-rps: 50`：**压测会先撞到这个，看到 503 不是应用的问题**

**中间件不在集群里。** PostgreSQL / Redis / RabbitMQ 全部由宿主 Homebrew 提供，Pod 通过 `host.docker.internal`（解析到 `192.168.65.254`）访问：

| 依赖 | 地址 | 凭证 |
|---|---|---|
| PostgreSQL 18 | `host.docker.internal:5432/shop` | `shop/shop` |
| Redis 8 | `host.docker.internal:6379` | 无 |
| RabbitMQ | `host.docker.internal:5672`，vhost `/shop` | `shop/shoppass` |

RabbitMQ 拓扑在 api/worker 启动时幂等声明：

```
shop.events (topic)   --order.created-->  shop.order.created
shop.dlx    (direct)  --.retry--------->  shop.order.created.retry   TTL 10s → 回主队列
                      --.dlq----------->  shop.order.created.dlq
```

## 从零重建时要手工做的一次性动作

代码和清单都在 Git 里，但下面三件事不在：

**① GitHub package 设为 public**（否则 Pod `ImagePullBackOff`）

仓库是 public，Actions 用 `GITHUB_TOKEN` 推的镜像**继承仓库可见性**，所以不需要额外 `write:packages` 权限。但 package 本身要手工设一次：

```
https://github.com/users/Jeffrey-Xu/packages/container/shop-gitops%2Fbackend/settings
https://github.com/users/Jeffrey-Xu/packages/container/shop-gitops%2Ffrontend/settings
```

或者建 `imagePullSecret` 走认证拉取。

**② 宿主中间件**

```bash
# PostgreSQL：建角色和库
psql -d postgres -c "CREATE ROLE shop LOGIN PASSWORD 'shop'"
psql -d postgres -c "CREATE DATABASE shop OWNER shop"

# RabbitMQ：建用户 + vhost + 权限，并启用指标插件
rabbitmqctl add_user shop shoppass
rabbitmqctl add_vhost /shop
rabbitmqctl set_permissions -p /shop shop ".*" ".*" ".*"
rabbitmq-plugins enable rabbitmq_prometheus
```

**③ ArgoCD 引导**

`argocd/install.yaml` 故意**不进 Git**（它是 bootstrap 用的 v3.6.1 清单），装完后手工 apply 5 个 Application：

```bash
kubectl apply -f argocd/install.yaml -n argocd    # 首次
kubectl apply -f argocd/application.yaml          # shop
kubectl apply -f monitoring/                      # 4 个平台 Application
```

## 本地访问

```bash
curl http://localhost/                  # 前端（Ingress 有无 host 兜底规则）
curl http://shop.localhost/             # 同一个入口
curl -D- http://localhost/api/products  # 看 X-Cache
curl http://localhost/api/stats
```

| 入口 | 说明 |
|---|---|
| `http://localhost/` 或 `http://shop.localhost/` | 业务站 |
| `http://argocd.localhost` | ArgoCD，admin 密码见 `argocd-initial-admin-secret` |
| `http://grafana.localhost` | Grafana `admin/admin` |

ArgoCD CLI 方式：`kubectl port-forward -n argocd svc/argocd-server 8080:80`。

## 可观测性

集群内 `monitoring` 命名空间，5 个 Application 全由 ArgoCD 管：

| 组件 | Chart / 来源 | 说明 |
|---|---|---|
| metrics-server | 3.14.0 | `kubectl top`，带 `--kubelet-insecure-tls` |
| kube-prometheus-stack | 93.4.0 | Prometheus + Grafana + Alertmanager + node-exporter + KSM + operator |
| loki | 7.6.0 | SingleBinary + filesystem，`retention_period: 168h` |
| alloy | 1.16.0 | DaemonSet，走 API 读容器日志，`stage.cri` + `stage.json` |
| monitoring-extras | 本仓库 `monitoring/extras` | mailpit、postgres-exporter、redis-exporter、业务大盘 |

**Metric 链路**：应用 `/metrics`（`prometheus/client_golang` + `promauto`）→ `ServiceMonitor`（15s）→ Prometheus（TSDB，7d / 4GB / 8Gi PVC）→ Grafana + 3 条 `PrometheusRule` 告警。

- 应用指标：`shop_http_requests_total`、`shop_http_request_duration_seconds`、`shop_cache_lookups_total`、`shop_orders_created_total`、`shop_orders_processed_total`、`shop_outbox_published_total`、`shop_payment_calls_total`（`result=succeeded|declined|error|disabled`）、`shop_queue_depth`、`shop_outbox_pending`；支付服务另有 `payment_charges_total`、`payment_charge_duration_seconds`。
- 宿主中间件：PG 与 Redis 用 exporter Pod 去 `host.docker.internal` 抓（`postgres-exporter`、`redis-exporter`），RabbitMQ 自带 `/metrics` 用 `additionalScrapeConfigs` 静态抓 `host.docker.internal:15692`。共 24 个 target 全 up。
- 大盘：`shop 业务总览`（`grafana.localhost` → Dashboards），10 块面板，含下单速率、5xx 比例、P50/95/99、缓存命中率、队列积压、worker 处理结果、outbox 投递结果，最后一行是支付的**两侧视角**（worker 调了多少次 / 网关自己记了多少笔）。

**Log 链路**：slog JSON → stdout → Alloy（`discovery.kubernetes` + `loki.source.kubernetes`，不挂 hostPath）→ `stage.cri` 拆行 → `stage.json` 取 `level`/`msg` → `level` 做成 label，`request_id`/`order_id`/`service` 放 structured metadata → `loki-gateway` → Loki（10Gi PVC）。

- `level` 基数只有 5 个可以当索引；`request_id` 每个请求都不同，做 label 会把索引撑爆，所以放 structured metadata。
- Alloy 里是 River 语法，**注释只能 `//`**。写成 `#` 会让 reload 静默失败并继续跑旧配置（ArgoCD 仍显示 Healthy），排查见「已知限制」。

**告警投递**：Alertmanager → SMTP → mailpit（`mailpit.localhost`）→ `oncall@shop.local`。默认 receiver 是 `null`（只 fire 不通知），现在改成 email；`route` 里那条 Watchdog 子路由仍指向 `null`，所以 receivers 里必须保留一个名为 `"null"` 的空接收器，否则 operator 报 `undefined receiver "null"` 拒绝渲染。

**Tracing：没有。** 全仓库没有 OTel SDK / Collector / Tempo。跨 `api → RabbitMQ → worker` 目前只能靠 `request_id` + `order_id` 在 Loki 里手工串；没有父子 span，也就无法回答“这 800ms 里 PG 占了多少”。

> ServiceMonitor **必须**带 `release: kube-prometheus-stack` 标签，否则静默不抓。

## 支付服务（payment-service）

一个独立的 Python/FastAPI 小服务，模拟第三方支付网关。它**不是** shop 的一部分进程，而是 worker 通过 HTTP 调的外部依赖。

```
POST /charge                 {order_id, amount_cents} → {status, payment_id, replayed}
GET  /payments/{order_id}    对账/回查
GET  /healthz /readyz /metrics
```

| 契约 | 实现方式 |
|---|---|
| **幂等** | `order_id` 是 `payments` 主键。先 `INSERT ... ON CONFLICT DO NOTHING` 再回查，而不是「先查后插」——后者并发下会双扣 |
| **持久化** | 扣款结果落 PG，进程重启不丢 |
| **可回查** | `GET /payments/{order_id}`，worker 可以拿它对账 |
| **拒付是业务结果** | 返回 200 + `status: "failed"` + `failure_reason`，不是 500 |

调用约定（`worker` 侧 `internal/payment`）：

- `PAYMENT_URL` 为空 → 不调网关，走内置模拟支付（本地不想起 Python 时就这样）
- 4xx 或 `status=failed` → `*DeclinedError`：**一次就取消订单**，不重试
- 超时（默认 3s）/ 连不上 / 5xx → 普通 error：走 `10s × 3` 退避重试，耗尽后 DLQ + 补偿

`PAYMENT_FAILURE_RATE`（0~1）按 `sha256(order_id)` 决定拒付，同一个订单每次判定一致，方便演示“重放不会变成成功”。

## 故障演练

```bash
# 制造 panic / 慢请求 / 500，用来在 Grafana 里看链路
curl -X POST 'http://localhost/api/dev/fault?type=panic'
curl -X POST 'http://localhost/api/dev/fault?type=slow'
curl -X POST 'http://localhost/api/dev/fault?type=error'

# 队列堆积：停掉 worker 后下单，队列涨、订单停在 CREATED
kubectl -n shop scale deploy/worker --replicas=0
# 恢复后积压自动消费完
kubectl -n shop scale deploy/worker --replicas=1

# 支付失败 → 3 次重试 → DLQ + 库存回补
redis-cli SET shop:demo:fail 1

# 真·拒付（payment 侧返回 failed）→ 一次就取消，不走重试
kubectl -n shop set env deploy/payment PAYMENT_FAILURE_RATE=1   # 测完 set env ... PAYMENT_FAILURE_RATE=0
# 注意 ArgoCD 开了 selfHeal，手改会被自动改回 git 里的值，要先把 app 的
# syncPolicy.automated 摘掉再改，测完恢复

# 网关整体不可用 → 连接拒绝 → 3 次重试后 CANCELLED + DLQ
kubectl -n shop scale deploy/payment --replicas=0

# 幂等：同一 key 第一次 201，第二次 200，同一个订单号
curl -X POST http://localhost/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' -d '{"productId":1,"quantity":1}'
```

## 已知限制

- `readyz` 把 Redis 当硬依赖：Redis 挂了整站 503，而实际上下单链路不需要它
- `MaxOpenConns(10)` 与 `MaxIdleConns(5)` 硬编码在 `store.go`，且 idle 应等于 open
- DSN 没设 `statement_timeout`，慢查询会一直占着连接
- `CreateOrder` 提交后又回查三次拿详情，多两次往返
- `orders(status)` 无索引，`/api/stats` 每次全表扫
- 单副本、无 HPA/PDB、无 NetworkPolicy
- 没有用户体系，`Idempotency-Key` 由前端生成
- 支付镜像是本地 `docker build` 的 `local1` tag，没走 CI；集群重置后要重新 build 并导入节点（`docker save | docker exec -i desktop-worker ctr -n k8s.io images import -`）
- backend 的 `paywiring1` 同样是本地补丁镜像：Docker Hub 不可达时拉不到 `golang:1.24-alpine`，于是宿主机 `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build` 之后，用现成镜像的运行层重新打包（见下文「Docker Hub 挂了怎么出镜像」）
- worker 调支付是**同步 HTTP**，3s 超时；支付服务慢会直接压到消费者的消费速率（prefetch=5 时最多 5 个并发卡在等）
- Alloy 的 River 配置写 `#` 注释会**静默**失效（reload 报错但继续跑旧配置，ArgoCD 依旧 Synced/Healthy）；排查方式是看 alloy 日志里的 `failed to reload config`
- 没有 tracing：跨 api→MQ→worker 只能靠 `request_id`/`order_id` 手工串日志

## Docker Hub 不可达时怎么出镜像

现象：`docker build` 报 `failed to fetch oauth token: Post "https://auth.docker.io/token": Bad Gateway`，
`docker pull golang:1.24-alpine` 同样失败。这是 Docker Hub 认证端点的问题，重试几次不一定好。

绕法：**不重建基础层，只换运行层里的二进制**。

```bash
cd backend
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o /tmp/paybuild/api    ./cmd/api
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o /tmp/paybuild/worker ./cmd/worker
```

`/tmp/paybuild/Dockerfile` —— `FROM` 用现成镜像的完整引用，**必须带 `@sha256:`**：
只写短 ID 时 BuildKit 会去 registry 做认证（然后 502）。

```dockerfile
FROM ghcr.io/jeffrey-xu/shop-gitops/backend:0364df2...@sha256:6aa0ec9a...
COPY --chown=10001:10001 api /app/api
COPY --chown=10001:10001 worker /app/worker
USER app
CMD ["/app/api"]
```

然后 `docker build` → 导入两个节点 → 改 `deploy/kustomization.yaml` 的 `newTag`。

架构要对：本机是 arm64，交叉编译目标写 `linux/arm64`
（`kubectl get nodes -o custom-columns=ARCH:.status.nodeInfo.architecture` 确认）。
`--chown` 保留 uid 10001，别让 `runAsNonRoot` + `readOnlyRootFilesystem` 的组合翻车。

## 推送排障：`git push` 报 502 / 超时怎么办

这台机器所有出网流量都走本地代理（`HTTPS_PROXY=http://127.0.0.1:63087`）。代理是
`CONNECT` 隧道，它自己连不上 github.com 时会把 502 原样吐给 git，看起来像"被墙"，
实际上**不是策略封锁**，通常是一过性上游问题或 DNS 解析到了不通的 POP。

先分层定位，别猜：

```bash
scripts/git-doctor.sh
```

它依次检查 DNS → 代理 CONNECT → api.github.com → gh 认证 → `git ls-remote`，
哪一层红就是哪一层的问题：

| 红的步骤 | 含义 | 处理 |
|---|---|---|
| 1 DNS | 解析不到，或解析到不通的 IP | `dscacheutil -flushcache`；反复不行就写 `/etc/hosts` 钉一个可用 IP |
| 2 CONNECT | 代理到 github.com:443 建不起隧道 | 等几分钟重试；持续不行用下面的 API 兜底 |
| 3 api | 兜底通道也不通 | 换网络（手机热点）验证是不是本地网络的问题 |
| 4 gh 认证 | token 过期 / 未登录 | `gh auth login`，需要 `repo` 权限 |
| 5 ls-remote | 能连但协议层协商失败 | 检查 `git remote -v` 地址、是否误设了 `http.proxy` |

### 兜底：走 API 落提交

`api.github.com` 和 `github.com` 是两套入口，前者通就可以绕开 git 协议：

```bash
python3 scripts/push-via-api.py
git fetch && git reset --hard origin/main   # 对齐历史，API 造的 commit hash 与本地不同
```

原理是用 Git Data API 依次建 blob → tree → commit → 移动 ref，语义等价于
`push --force-with-lease`。脚本会检查远端有没有本地不存在的文件，有就拒绝覆盖。

### 别做的事

- **不要 `git config --global http.proxy ""`**：这里必须走代理，清掉只会变成彻底连不上
- **不要改 `~/.ssh` 去试 SSH**：22 端口本来就出不去，SSH over 443 在代理下同样要 CONNECT
- **不要反复 `git push -f`**：CI 的 bump job 会回写 `deploy/kustomization.yaml`，
  硬推会把它的更新打掉，ArgoCD 随后会看到一个倒退的 tag
