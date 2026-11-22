# shop-gitops

一个刻意做小的电商后端，用来把 **缓存 + 消息队列 + 事务性 outbox + 可观测性** 这几件事在真集群里跑通。

前端是静态站，后端是 Go（`api` / `worker` / `account` 三个入口同一个镜像），数据落宿主机的 PostgreSQL，Redis 做读缓存，RabbitMQ 做异步事件。另外有两个独立服务：**Python/FastAPI 支付服务**（`payment-service/`）和 **Node.js 履约服务**（`fulfillment-service/`），都用同一个 PG 的不同表。交付走 **GitHub Actions 构建 + ArgoCD 同步**。

三个服务分别用三种语言不是炫技，是为了把「服务边界」这件事做得足够硬：

| 服务 | 语言 | 拥有什么 | 边界怎么守 |
|---|---|---|---|
| `api` / `worker` / `account` | Go | `orders` `order_items` `order_events` `outbox` `products` `users` | 订单表的唯一写入口 |
| `payment-service` | Python | `payments` | 幂等扣款，可回查 |
| `fulfillment-service` | Node | `shipments` | 只回写状态，**不碰 orders 表** |

`fulfillment` 想改订单状态，必须调 `api` 的 `/api/internal/shipments`。共享数据库看起来省事，但它把两边的 schema 焊死在一起——Go 改个列名，Node 运行时才炸，编译期完全看不见。


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
| `backend/cmd/worker/` | RabbitMQ 消费者入口，负责扣款（管钱） |
| `backend/cmd/account/` | 账号服务入口，注册/登录/签发 JWT |
| `backend/cmd/backfill/` | 一次性运维工具：给「已 PAID 但无 `order.paid` 事件」的存量订单补 outbox，默认 dry-run |
| `backend/cmd/replay/` | 运维工具：把 DLQ 里的消息重投回主队列（清 attempts、保留 traceparent），默认 dry-run |
| `backend/internal/` | `config` `obs` `httpx` `cache` `store` `broker` `outbox` `api` `worker` `account` `auth` `payment` |
| `backend/internal/store/migrations/` | `go:embed` 的 SQL 迁移，启动时同步执行，失败即退出 |
| `frontend/` | Vite + 原生 JS，构建成 `dist/` 由 nginx 托管。`src/art.js` 画商品矢量图，`src/cart.js` 管购物车，`src/api.js` 收口所有请求 |
| `payment-service/` | Python 3.12 + FastAPI，模拟第三方支付网关（幂等扣款 + 落库 + 回查） |
| `fulfillment-service/` | Node 22 + Fastify，消费 `order.paid`，建发货单、推进物流、回写订单 |
| `deploy/` | Kustomize：namespace、api/worker/account/frontend/payment/fulfillment、ingress、monitoring |
| `argocd/` | ArgoCD 引导清单（`install.yaml` 不进 Git） |
| `monitoring/` | metrics-server / kube-prometheus-stack / loki / alloy / tempo 的 Application |
| `monitoring/extras/` | mailpit、宿主 PG/Redis exporter、Grafana 业务大盘（由 `monitoring-extras` 这个 Application 管） |
| `scripts/` | `git-doctor.sh` 推送链路诊断、`push-via-api.py` 兜底提交、`build-frontend-local.sh` 本地出前端镜像 |
| `.github/workflows/ci.yaml` | 矩阵构建 + 回写 tag |

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/hello` | 返回 Pod 名与版本，验证分流 |
| GET | `/api/products` | 商品列表，走 Redis，响应头带 `X-Cache: HIT\|MISS`。支持 `?category=` `?q=` `?sort=sales\|price_asc\|price_desc\|rating`，响应里带 `categories`（品类+数量）和 `total`/`catalog` |
| GET | `/api/products/{id}` | 单商品 |
| POST | `/api/orders` | 下单，扣库存 + 建单 + 写 outbox，一个事务。支持 **一单多品**：`{"items":[{"productId":1,"quantity":2},…]}`；旧的 `{"productId":1,"quantity":1}` 仍然可用 |
| GET | `/api/orders` | **需登录**：当前用户自己的订单列表（含明细与状态历史），只按 token 里的 uid 查 |
| GET | `/api/orders/{id}` | 订单详情含状态迁移历史 |
| GET | `/api/stats` | 缓存命中率、队列深度、各状态订单数，前端 footer 用 |
| POST | `/api/dev/fault?type=panic\|slow\|error` | 故障注入，仅 `DEV_MODE=true` 时存在 |
| GET | `/metrics` | Prometheus 指标（api 与 worker 各一份） |
| GET | `/healthz` | liveness，不查任何依赖 |
| GET | `/readyz` | readiness，PG 是**硬**依赖（失败 503），Redis 是**软**依赖（失败仍 200，响应里 `redis: "degraded"`） |
| POST | `/api/internal/shipments` | **服务间专用**：履约服务回写 `SHIPPED`/`DELIVERED`，需 `X-Internal-Token` |

> `healthz` 与 `readyz` 分开：依赖挂了应该摘流量，不该被 liveness 反复重启。
>
> 两个依赖的地位不一样，这不是漏改：**PG 挂了几乎每个接口都会 500**，留着接流量只是放大错误；
> **Redis 只承担商品目录缓存，下单链路一次都不读它**，挂了应该穿透回源、吞吐掉一截但仍然能下单。
> 把它当硬依赖的后果是：一次缓存抖动让整个 api 被摘流量 —— 明明能服务，自己主动不服务了。
> 判据是**调用路径**，不是组件本身：将来出现真正依赖 Redis 的强一致需求（分布式锁、限流），这个判断要跟着改。
> 降级状态会体现在 `shop_dependency_up{dependency="redis",mode="soft"}`、`shop_dependency_flaps_total`
> 和一条只在**状态翻转时**打的 `redis_degraded` 日志上（探针每 5 秒一次，每次都打就是噪音）。
>
> `/api/internal/*` 是靠共享密钥挡的（`INTERNAL_TOKEN` 为空时该端点直接 404）。
> 生产环境还应该在 Ingress 层再拦一次——应用层鉴权不该是唯一一道门。

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

账号服务（集群内 `svc/account:8080`，集群外走 `/auth` 前缀）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/register` | `{email, password, displayName}` → 201 + JWT |
| POST | `/login` | `{email, password}` → 200 + JWT，密码错/用户不存在都返回同一句 401 |
| GET | `/me` | 需 `Authorization: Bearer <token>`，返回当前用户 |
| GET | `/healthz` `/readyz` `/metrics` | 存活 / 就绪（查 PG）/ 指标 |

```bash
TOKEN=$(curl -s -X POST http://localhost/auth/register -H 'Content-Type: application/json' \
  -d '{"email":"a@b.com","password":"demopass123","displayName":"张三"}' | jq -r .token)
curl -s http://localhost/auth/me -H "Authorization: Bearer $TOKEN"

# 带 token 下单 → 订单归属到这个用户；不带也能下单（匿名）
curl -s -X POST http://localhost/api/orders -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: $(uuidgen)" \
  -d '{"items":[{"productId":1,"quantity":1},{"productId":15,"quantity":3}]}' \
  | jq '{id,userId,status,totalCents,items}'
```

履约服务（集群内 `svc/fulfillment:8080`，集群外走 `/fulfill` 前缀）：

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/shipments` | 最近 50 张发货单 |
| GET | `/shipments/{orderId}` | 单个订单的发货单（承运商、运单号、内部物流状态、尝试次数） |
| POST | `/shipments/{orderId}/advance` | **演示用**：立刻推进一步，不用等 ticker |
| GET | `/healthz` `/readyz` `/metrics` | 存活 / 就绪（查 PG + 消费连接）/ 指标 |

```bash
curl -s http://localhost/fulfill/shipments/34 | jq '{status,tracking_no,carrier,attempts}'
curl -s -X POST http://localhost/fulfill/shipments/34/advance   # 手动催一单
```

> `/fulfill` 前缀会把 `POST .../advance` 一起暴露出去，演示环境故意留着。生产要挡掉。

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
   ├─ 同一事务：INSERT outbox (topic='order.paid')   ← 履约的触发点
   └─ ack → 订单变 PAID（order_events.note 里带 payment_id）
```

**`order.paid` 必须和状态变更在同一个事务里写 outbox**。先 commit 再发消息的话，中间进程挂掉就会出现「钱收了、货永远不发」——而这种故障在日志里几乎看不出来。已付款的订单卡在 PAID 是最贵的一种 bug。


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

## 支付之后：从 PAID 到 DELIVERED

钱收完了，接下来是货。这一段由 **Node 写的履约服务**负责，它是整条链路里唯一一个非 Go 的写路径。

```
outbox relay ──order.paid──▶ shop.events ──▶ shop.order.paid
   │  （payload 里带 items 快照：履约服务不回查 orders 表）
   ▼
fulfillment（Node）
   ├─ INSERT shipments ... ON CONFLICT (order_id) DO NOTHING
   │     └─ 没返回行 = 重复投递，ack 走人（不是错误）
   ├─ ack（建单是毫秒级，不能把发货这种长流程压在消息处理里）
   │
   └─ 每 6s 扫一次到期的发货单
        PENDING ──▶ SHIPPED ──▶ IN_TRANSIT ──▶ DELIVERED
                      │            │              │
                      └────────────┴──────────────┘
                                   │
                    POST /api/internal/shipments  (X-Internal-Token)
                                   │
                                   ▼
                    api：UPDATE orders WHERE status=<前置状态>   ← 乐观锁
                         + INSERT order_events（UNIQUE 兜底）
```

三个设计选择值得单独说：

**1. 内部状态比外部状态细。** `shipments.status` 有四档，`orders.status` 只有 `SHIPPED`/`DELIVERED`。`SHIPPED` 和 `IN_TRANSIT` 都映射到订单的 `SHIPPED`。反过来（外部比内部细）会逼着内部编造假状态。

**2. 推进用「定时扫数据库」，不用内存队列。** 进程重启后内存里的待办就没了，那些 PENDING 的发货单会永远停着；而数据库里的 `next_advance_at` 不会丢。代价是一个 tick 周期的延迟——对物流这种本来就以小时计的流程完全可接受。

**3. 回写失败就回滚发货单状态。**

```js
await advanceShipment(id, from, to)        // 先改库
try {
  await reportShipment({ orderId, status })  // 再回写订单
} catch (err) {
  if (err.retryable) await rollbackAdvance(id, from, to, err.message)  // 5s 后再试
  else               await markFailed(id, err.message)                 // 订单被取消了，停下来等人看
}
```

回滚是安全的，因为 **api 侧的回写是幂等的**：`WHERE status = <前置状态>` 加 `order_events` 的唯一约束，重复回写只会返回 `duplicate`。

失败分类和 worker 那边是同一套逻辑：`4xx` = 请求本身有问题，重试没意义；`5xx` / 超时 / 连不上 = 值得再试。

### 存量订单：补 `order.paid` 事件

履约服务是后加的，它上线之前 worker 扣款成功只把订单改成 `PAID`，**没有写 `order.paid` 事件**。这批单会永久停在 `PAID`——钱收了，货永远不发。本仓库实测时就撞上了 31 张。

`backend/cmd/backfill` 用来补这个洞：

```bash
# 先看要补哪些，默认 dry-run，不改数据
DATABASE_URL=postgres://shop:***@127.0.0.1:5432/shop?sslmode=disable \
  go run ./cmd/backfill

# 确认无误后写入
  ... go run ./cmd/backfill -apply
```

它**只往 `outbox` 插记录，绝不直接 `UPDATE orders`**。原因：`orders` 只允许一个写入口，直接改状态会绕过状态机和 `order_events` 审计。补 outbox 等于「让这些单重新走一遍正常链路」——relay → `shop.order.paid` → fulfillment 建单 → 逐步推进 → 回写订单，每一步都是既有逻辑，没有新增特殊路径。

三处防重：

| 条件 | 挡住什么 |
|---|---|
| `status = 'PAID'` | 已推进到 SHIPPED/DELIVERED 的单 |
| `NOT EXISTS` outbox 里同 `orderId` 的 `order.paid` | 已经被补过、或本来就正常收到事件的单 |
| `NOT EXISTS` `order_events` 里的 `SHIPPED` | 正在履约途中、只是还没回写完的单 |

写入时每条再判一次 `NOT EXISTS`（扫表和写入之间有窗口），重复的 `order.paid` 虽然不会造成重复发货（`createShipment` 是 `ON CONFLICT DO NOTHING`），但没必要制造噪音。

支付号从 `order_events` 的 note 里正则捞（worker 当初写的是 `worker: payment captured (pay_xxx)`），捞不到就留空——不编造。

实测结果：31 张单写入后 **50 秒内**全部走完 `PAID → SHIPPED → DELIVERED`，最终 36 张发货单对 36 张唯一订单，无重复事件，`PAID` 归零。

## 店面：前端这一层做了什么

页面从「三张卡片 + 一个下单按钮」变成了能逛的店：吸顶导航、品类筛选、搜索、排序、
商品详情、购物车抽屉、结算、实时状态时间轴、我的订单。几个决定值得写下来。

**1. 商品图是矢量画的，不是图片文件。**
商品表里存的是 `art` 这个标识（`keyboard`/`chair`/…），不是图片 URL。前端的 `src/art.js`
按标识现画 SVG。理由有三条：这个集群经常拉不到外部图床，图片一挂就是一片灰块；
矢量图跟 CSS 一起进包，离线永远画得出来；换主题色不用重新出图。
把「画法」放在前端而不是数据库里，也避免商品数据里混进构建产物路径（换 CDN 就要改数据）。

**2. 筛选在内存里做，不往 SQL 下推。**
`/api/products?category=&q=&sort=` 的过滤发生在 Go 里，发生在缓存之后。这样缓存键仍然只有
`shop:products:list` 一个：按 category 做缓存键会让键数量爆炸，而且下单后的失效逻辑要跟着改
（现在只删一个键）。目录只有十几行的前提下，全量反序列化的开销可以忽略。
目录涨到几百行、或者要接搜索引擎时，这个决定就该反过来。

**3. 购物车只存 `{id, qty}`，不存价格。**
价格必须每次从后端拿。存下来就会遇到「改价之后购物车里还是旧价」，结算金额和商品页对不上。
商品下架时不静默删除，而是标出来让用户自己移除——金额变了得让人知道为什么。

**4. 幂等键和「这一次点击」绑定，在调用处生成。**
放在 `api.createOrder` 里生成的话，网络超时重试会换一个键，等于没有幂等。

**5. 状态时间轴读的是 `order_events`，不是前端推算的。**
四步（已提交 / 已支付 / 已发货 / 已签收）的完成状态和每步时间都来自 `history` 数组，
前端只负责翻译。前端自己按 status 猜的话，出现「SHIPPED 但没有 SHIPPED 事件」这种脏数据时
界面会撒谎。终态不再高亮成「进行中」——已完成还亮橙色会被读成出事了。

**6. 路由用 hash，两页不值得引 router 库。**
收益是刷新不丢页面（`#/orders` 可以直接分享）。代价是不能做 SSR，这个站不需要。

结算走一单多品，`POST /api/orders` 一次提交整个购物车。部分成交需要「部分退款 + 部分发货」的
模型，复杂度不成比例，所以后端语义是**要么全成要么全不成**：任何一行库存不足，整单回滚。

## 数据模型

| 表 | 关键字段 | 说明 |
|---|---|---|
| `products` | `price_cents BIGINT`、`stock`、`version`；展示字段 `description` / `category` / `art` / `rating` / `sold` / `original_price_cents` | 金额用整数分，不用浮点。`art` 是前端插画标识，不是图片 URL |
| `orders` | `status`、`total_cents`、`idempotency_key UNIQUE`、`user_id`、`tracking_no` | 状态机 `CREATED → PAID → SHIPPED → DELIVERED`，另支 `CANCELLED` |
| `order_items` | `unit_price_cents` | 下单时快照单价，改价不影响历史单 |
| `order_events` | `from_status`、`to_status`、`UNIQUE(order_id,to_status)` | 状态迁移审计，日志之外的第二证据 |
| `outbox` | `topic`、`payload jsonb`、`published_at`、`attempts` | 未投递事件队列 |
| `users` | `email`、`password_hash`、`lower(email)` 唯一索引 | 账号服务拥有；哈希永不出 `store` 包 |
| `payments` | `order_id` 主键、`status`、`payment_id` | 支付服务拥有 |
| `shipments` | `order_id UNIQUE`、`tracking_no`、`status`、`next_advance_at` | 履约服务拥有 |

**库存只在 PostgreSQL 扣，Redis 不参与扣减。** 缓存只负责读得快，一致性交给数据库。

一单多品的扣减按 `product_id` 升序逐行执行，让并发下单的加锁顺序一致——不然 A 单锁 1 再锁 2、
B 单锁 2 再锁 1，就是教科书式的死锁。任何一行库存不足就整单回滚，不做部分成交。

`orders` 上有三处细节值得注意：

- `user_id` 允许 NULL —— 匿名下单是**受支持**的路径，不是漏洞。前端不登录也能买，只是「我的订单」里看不到。
- `tracking_no` 逻辑上归履约服务，这里冗余一份纯粹为了读路径：`GET /api/orders/{id}` 不能为了显示一个运单号就跨服务调一次 HTTP。
- `status` 的取值靠 CHECK 约束守着。加发货状态时必须先 `DROP CONSTRAINT` 再 `ADD`，因为 PG 不允许原地改 CHECK 的取值集合。

**越权防护**：订单一旦有 `user_id`，只有本人能查，其他人一律拿 404（不是 403——403 等于告诉对方「这个 id 存在，只是不是你的」）。

## 部署形态

集群里一个 `shop` 命名空间，Kustomize 渲染出 25 个对象：

```
Namespace shop
ConfigMap shop-config        APP_VERSION / LOG_LEVEL / CACHE_TTL / DEV_MODE
                             REDIS_ADDR / MQ_PREFETCH / MQ_MAX_ATTEMPTS
                             PAYMENT_URL / PAYMENT_TIMEOUT / JWT_ISSUER / JWT_TTL
Secret    shop-secret        database-url / rabbitmq-url / jwt-secret / internal-token
Deployment api         1 副本  /app/api          req 50m/64Mi   limit 500m/256Mi
Deployment worker      1 副本  /app/worker       req 30m/48Mi   limit 300m/192Mi
Deployment account     1 副本  /app/account      req 30m/48Mi   limit 400m/192Mi
Deployment frontend    1 副本  nginx:1.27        req 20m/32Mi   limit 200m/128Mi
Deployment payment     1 副本  uvicorn           req 50m/96Mi   limit 300m/256Mi
Deployment fulfillment 1 副本  node 22           req 30m/64Mi   limit 300m/256Mi
Service   api / frontend / worker / account / payment / fulfillment
Ingress   shop               shop.localhost + 无 host 兜底
          account            /auth(/|$)(.*)     → rewrite /$2
          payment            /pay(/|$)(.*)      → rewrite /$2
          fulfillment        /fulfill(/|$)(.*)  → rewrite /$2
ServiceMonitor api / worker / account / payment / fulfillment
PrometheusRule shop         5 条告警
```

- 六个 Deployment 都是 `replicas: 1` + `strategy: Recreate`。本地学习环境，不做高可用
- Go 三个角色以 uid `10001` 非 root 运行；Node 服务用镜像内置的 `node` 用户（uid 1000）
- 全部 `readOnlyRootFilesystem` + drop 所有 capability。Node 额外挂了一个 `emptyDir` 到 `/tmp`，否则写临时文件会失败
- 探针：readiness `/readyz`（5s/5s，失败 3 次摘流量）、liveness `/healthz`（20s/10s）
- `fulfillment` 的 `/readyz` 同时检查 PG **和** RabbitMQ 消费连接：消费断了就不该算 ready，否则新订单会悄无声息地堆在队列里
- Ingress 带 `limit-rps: 50`：**压测会先撞到这个，看到 503 不是应用的问题**

**中间件不在集群里。** PostgreSQL / Redis / RabbitMQ 全部由宿主 Homebrew 提供，Pod 通过 `host.docker.internal`（解析到 `192.168.65.254`）访问：

| 依赖 | 地址 | 凭证 |
|---|---|---|
| PostgreSQL 18 | `host.docker.internal:5432/shop` | `shop/shop` |
| Redis 8 | `host.docker.internal:6379` | 无 |
| RabbitMQ | `host.docker.internal:5672`，vhost `/shop` | `shop/shoppass` |

RabbitMQ 拓扑在 api/worker 启动时幂等声明（**Node 侧只消费、不声明**——两边用不同参数声明同一个队列会直接 `PRECONDITION_FAILED` 关连接）：

```
shop.events (topic)   --order.created-->  shop.order.created
                      --order.paid----->  shop.order.paid      ← 履约服务消费
shop.dlx    (direct)  --.retry--------->  shop.order.created.retry   TTL 10s → 回主队列
                      --.dlq----------->  shop.order.created.dlq
                      --.retry--------->  shop.order.paid.retry      TTL 10s → 回主队列
                      --.dlq----------->  shop.order.paid.dlq
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

`argocd/install.yaml` 故意**不进 Git**（它是 bootstrap 用的 v3.6.1 清单），装完后手工 apply，Application 全是「一个 yaml 一个 App」：

```bash
kubectl apply -f argocd/install.yaml -n argocd    # 首次
kubectl apply -f argocd/application.yaml          # shop
kubectl apply -f monitoring/*.yaml                # 7 个平台 Application（含 tempo）
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
| `http://localhost/` 或 `http://shop.localhost/` | 业务站（右上角登录/注册；`/#login` 直接弹登录框） |
| `http://localhost/auth/...` | 账号服务：`/auth/register`、`/auth/login`、`/auth/me` |
| `http://localhost/fulfill/...` | 履约服务：`/fulfill/shipments`、`/fulfill/shipments/{orderId}` |
| `http://localhost/pay/...` | 支付服务：`/pay/charge`、`/pay/payments/{order_id}` |
| `http://argocd.localhost` | ArgoCD，admin 密码见 `argocd-initial-admin-secret` |
| `http://grafana.localhost` | Grafana `admin/admin` |
| `http://mailpit.localhost` | 本地邮件收件箱（Alertmanager 告警落这里） |

ArgoCD CLI 方式：`kubectl port-forward -n argocd svc/argocd-server 8080:80`。

## 可观测性

集群内 `monitoring` 命名空间，7 个 Application 全由 ArgoCD 管：

| 组件 | Chart / 来源 | 说明 |
|---|---|---|
| metrics-server | 3.14.0 | `kubectl top`，带 `--kubelet-insecure-tls` |
| kube-prometheus-stack | 93.4.0 | Prometheus + Grafana + Alertmanager + node-exporter + KSM + operator |
| loki | 7.6.0 | SingleBinary + filesystem，`retention_period: 168h` |
| alloy | 1.16.0 | DaemonSet，走 API 读容器日志，`stage.cri` + `stage.json` |
| tempo | 1.24.4（app 2.9.0） | SingleBinary + filesystem，`retention: 24h`，OTLP 4317/4318 |
| monitoring-extras | 本仓库 `monitoring/extras` | mailpit、postgres-exporter、redis-exporter、业务大盘 |

**Metric 链路**：应用 `/metrics`（`prometheus/client_golang` + `promauto`）→ `ServiceMonitor`（15s）→ Prometheus（TSDB，7d / 4GB / 8Gi PVC）→ Grafana + 5 条 `PrometheusRule` 告警。

- 应用指标：`shop_http_requests_total`、`shop_http_request_duration_seconds`、`shop_cache_lookups_total`、`shop_orders_created_total`、`shop_orders_processed_total`、`shop_outbox_published_total`、`shop_payment_calls_total`（`result=succeeded|declined|error|disabled`）、`shop_queue_messages{queue}`（6 个队列全采）、`shop_outbox_pending`、`shop_outbox_oldest_seconds`、`shop_auth_events_total`（`action=register|login`）、`shop_shipment_callbacks_total`（`result=ok|duplicate|invalid|not_found|error`）。
  - `shop_queue_messages` 覆盖两个主队列、两个重试队列、两个 DLQ。只盯主队列时 DLQ 完全不可见，而「主队列空 + DLQ 非空」正是「系统看着正常、钱收不回来」的形态。
  - `shop_outbox_oldest_seconds` 比 `pending` 更能说明链路在不在动：broker 半通时积压涨得很慢，计数看着还行，最老那条已经躺了几分钟。relay 健康时这个值只有几秒（一个轮询周期）。
- 告警：5xx 比例 >5%、主队列积压 >10、**任一 DLQ >0（1 分钟，critical）**、**outbox 最老一条 >60s（2 分钟，critical）**、outbox 积压 >20（warning）。后两条的区分是刻意的：量大但年龄小是吞吐跟不上，年龄单调增长才是卡死。
- 支付服务指标：`payment_charges_total`、`payment_charge_duration_seconds`。
- 履约服务指标（Node，`prom-client`）：`fulfillment_shipments_created_total`、`fulfillment_advances_total`、`fulfillment_callback_duration_seconds`、`fulfillment_orders_in_flight`、`fulfillment_consumer_connected`，外加 `collectDefaultMetrics` 的进程级指标——其中 **event loop lag 对 Node 特别重要**，一个同步阻塞就能让所有请求延迟抖一下，而这是唯一能看见它的地方。
- 宿主中间件：PG 与 Redis 用 exporter Pod 去 `host.docker.internal` 抓（`postgres-exporter`、`redis-exporter`），RabbitMQ 自带 `/metrics` 用 `additionalScrapeConfigs` 静态抓 `host.docker.internal:15692`。共 29 个 target 全 up。
- 大盘：`shop 业务总览`（`grafana.localhost` → Dashboards），15 块面板：下单速率、5xx 比例、P50/95/99、缓存命中率、队列积压、worker 处理结果、outbox 投递结果、支付的**两侧视角**（worker 调了多少次 / 网关自己记了多少笔）、履约的发货建单与状态推进、在途单量、回写耗时、账号注册登录结果。

一个多语言的坑：`prom-client` 的 `Gauge.get()` 返回结构跟着版本变，**健康检查不要反过来读指标库的内部结构**（`fulfillment` 的 `/readyz` 就这么崩过一次）。指标是给 Prometheus 的，业务判断自己维护状态。

**Log 链路**：slog JSON → stdout → Alloy（`discovery.kubernetes` + `loki.source.kubernetes`，不挂 hostPath）→ `stage.cri` 拆行 → `stage.json` 取 `level`/`msg` → `level` 做成 label，`request_id`/`order_id`/`service`/`trace_id` 放 structured metadata → `loki-gateway` → Loki（10Gi PVC）。

三种语言的日志格式是**对齐过的**，Alloy 那边一条 pipeline 通吃：

| 服务 | 库 | 关键处理 |
|---|---|---|
| Go（api/worker/account） | `log/slog` | JSONHandler 直接输出 `{"level":"info","msg":...}` |
| Python（payment） | `logging` + 自定义 Formatter | 自己拼 JSON，字段名抄 slog |
| Node（fulfillment） | `pino` | `formatters.level: (label) => ({level: label})` —— pino 默认输出 `"level":30` 这种数字，不改就提不出 label |

- `level` 基数只有 5 个可以当索引；`request_id` 每个请求都不同，做 label 会把索引撑爆，所以放 structured metadata。
- Alloy 里是 River 语法，**注释只能 `//`**。写成 `#` 会让 reload 静默失败并继续跑旧配置（ArgoCD 仍显示 Healthy），排查见「已知限制」。

**告警投递**：Alertmanager → SMTP → mailpit（`mailpit.localhost`）→ `oncall@shop.local`。默认 receiver 是 `null`（只 fire 不通知），现在改成 email；`route` 里那条 Watchdog 子路由仍指向 `null`，所以 receivers 里必须保留一个名为 `"null"` 的空接收器，否则 operator 报 `undefined receiver "null"` 拒绝渲染。

**Trace 链路**：OTel SDK（Go / Node / Python 各一套）→ OTLP/HTTP → Tempo → Grafana。一单从 `POST /api/orders` 到发货回写，是一条 trace。下面是真跑一单抓下来的（订单 53，`duration 2260ms`，其中 1.8s 是两次 outbox 轮询的等待）：

```
api            POST /api/orders                      SERVER      72.5ms
 ├─ sql.conn.begin_tx                                CLIENT
 ├─ UPDATE products                                  CLIENT      12.5ms  ← 扣库存（条件更新 + RETURNING）
 ├─ INSERT orders                                    CLIENT       9.2ms
 ├─ INSERT order_items                               CLIENT       5.1ms
 ├─ INSERT order_events                              CLIENT       2.1ms
 ├─ INSERT outbox                                    CLIENT       8.6ms  ← traceparent 一起落库
 ├─ sql.tx.commit                                    CLIENT       2.7ms
 └─ publish order.created                             PRODUCER    +216ms  ← outbox relay 轮到它
     └─ consume shop.order.created                    CONSUMER    368.2ms ← worker
         ├─ SELECT orders                             CLIENT       7.8ms  ← 幂等闸门：只有 CREATED 才处理
         ├─ POST /charge                              CLIENT      63.8ms
         │   └─ POST /charge                          SERVER      43.3ms  ← payment（Python）
         ├─ UPDATE orders                             CLIENT     140.1ms
         ├─ INSERT order_events                       CLIENT       2.8ms
         ├─ SELECT orders / SELECT order_items        CLIENT
         ├─ INSERT outbox                             CLIENT
         ├─ sql.tx.commit                             CLIENT
         └─ publish order.paid                        PRODUCER    +2119ms
             └─ consume shop.order.paid               CONSUMER     35.3ms ← fulfillment
                 ├─ pg.query:INSERT shop              CLIENT      32.4ms  ← pg（Node 侧叫法不同）
                 └─ POST /api/internal/shipments      CLIENT
```

> Go 侧的 SQL span 来自 **`otelsql`**（包在 `database/sql` 这一层，覆盖全部查询位置）。span 名是「操作 + 表名」（`SELECT orders` / `INSERT outbox`），**不是整条 SQL**——后者参数不同就是新名字，基数会随数据量增长，Tempo 里按名字聚合立刻失效。完整语句在 `db.query.text` 属性里。
> 采集上刻意关掉了 `Ping`（readyz 每 5 秒一次）、`RowsNext`（每行一个 event）、`sql.rows`（实测一单里 6 个同名细条，把真正的语句淹掉）、prepare/reset_session（连接池内部动作）。原则和「探针不采 span」一样：**能被定时器驱动的东西不要进 trace**。

四个必须记住的决定：

1. **`traceparent` 跟着 outbox 行落库**（`outbox.traceparent`，迁移 0004）。relay 是 2 秒轮询驱动的，下单请求的 span 到那时早就结束了，context 传不过去。不落库，链路会在「事务 → 轮询 → 发布」这里断成两条互不相干的 trace。这是整条链路里唯一一处不能靠传播、只能靠持久化的地方。
2. **AMQP 的注入/还原写在 `broker` 包里**，不在业务代码里。`PublishTo` 开 producer span 并注入消息头，`Consume` 从消息头还原 ctx 再交给 handler —— 于是重试链路自动接在上一段下面（一条 trace 里能看完三次失败加一次成功）。amqp091 的 `Headers` 是 `map[string]any`，值里混着 `int32`，所以有个 `amqpCarrier` 只把字符串当传播载体。
3. **重试会拉长同一条 trace**。每次重试是「再投一条新消息」，带着上一段的 `traceparent`，所以第 4 次尝试是第 3 次的子 span。好处是失败现场完整，代价是 trace 时长包含退避等待（2s 档 + 4s 档，各带 0.5~1.5 倍抖动）。
4. **物流推进单独成 trace**，不挂在支付那条上。`fulfillment` 的定时器隔几十秒推一档，塞进同一条 trace 会让它长得没法读，也会毁掉「下单链路耗时」这个指标。两条 trace 用 `order.id` 关联。

服务名与采样：`OTEL_SERVICE_NAME` 按 Deployment 给（三个 Go 服务共用一个镜像），endpoint/采样率放在 `shop-config` 一份。默认全采（`OTEL_TRACES_SAMPLER_ARG=1.0`）—— 本地量小，演示时不能出现「这单刚好没被采到」。批量导出 2 秒（默认 5 秒会让人以为链路没接上）。探针与 `/metrics` 不采，否则 Tempo 里绝大多数 span 是 kubelet。

**日志 ↔ 链路**两个方向都通了，靠的是显式写死的 `uid`：

| 方向 | 配置 | 说明 |
|---|---|---|
| 日志 → trace | Loki `derivedFields` | 正则抓日志里的 `"trace_id":"<32hex>"`，点一下跳 Tempo |
| trace → 日志 | Tempo `tracesToLogsV2` | `{namespace="shop"} \|= "${__span.traceId}"`，一单跨 5 个服务，按命名空间捞比按服务捞有用 |

> Grafana 的 provisioning 文件里，`$` 必须写成 `$$`：Grafana 读文件时会把 `$` 当环境变量插值，直接写 `${__value.raw}` 会查不到这个环境变量、被替换成空串，链接点开是空的而且不报错。反过来写成 `$$` 时，Helm 不会帮你还原 —— 它原样存进去，由 Grafana 还原。这两种写法我都试过，只有 `$$` 是对的。

> 数据源必须显式给 `uid`（`loki` / `tempo`）。默认是随机串，两个方向的跳转都按 uid 引用，随机串意味着「重建一次 Grafana 就断一次」，且断得没有报错。

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
- 超时（默认 3s）/ 连不上 / 5xx → 普通 error：走指数退避重试（`2s → 4s`，各乘 `[0.5,1.5)` 随机系数），3 次尝试后 DLQ + 补偿

退避为什么不固定：固定 10 秒时，同一秒失败的一批订单会在第 10、20 秒整批回来 —— 那不是重试，是把一次瞬时抖动复制成三波尖峰，下游刚缓过来就被同一批请求再打一次。延迟由**消息自己**带（`PublishDelayed` 设 per-message `Expiration`），因为队列级 `x-message-ttl` 是队列常量，没法按「第几次重试」变化。队列级 10s 保留为上限兜底。

代价写在这里：RabbitMQ 只在**队头**判定消息过期，长延迟消息会压住排在后面的短延迟（head-of-line blocking）。本项目只有 2s/4s 两档、偏差上限约一个档位，而抖动的目的本来就是「别同时回来」，这点偏差不影响目标。要精确到毫秒得上 `rabbitmq_delayed_message_exchange` 插件或每个档位一个队列 —— 不是在单队列上继续拧 per-message TTL。

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

# ---- 死信怎么捞回来 ----

# 先看 DLQ 里有什么（默认 dry-run：只读不投，队列状态不变）
kubectl -n shop exec deploy/api -- /app/replay
# 只处理一类事件 / 限量
kubectl -n shop exec deploy/api -- /app/replay -queue order.paid -limit 20
# 确认后重投
kubectl -n shop exec deploy/api -- /app/replay -apply

# 重投后看两个地方：
#   worker 日志 attempt=1（说明 attempts 被清了，不是带着 4 再去撞一次）
#   orders 表里这些单是否还在 CREATED
#
# 注意：如果订单在重试耗尽时已经被补偿成 CANCELLED，重投不会「复活」它 ——
# worker 的幂等闸门会直接 skip_duplicate_event。这是对的：
# 消息重放 ≠ 业务重做，要不要重新下单是人的决定。

# 幂等：同一 key 第一次 201，第二次 200，同一个订单号
curl -X POST http://localhost/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' -d '{"productId":1,"quantity":1}'

# ---- 履约侧 ----

# 停掉履约服务后下单：订单会停在 PAID，发货单不建，
# shop.order.paid 队列开始积压（监控里 queue depth 涨、in_flight 为 0）
kubectl -n shop scale deploy/fulfillment --replicas=0
# 恢复后积压自动消费，PAID 的订单陆续走到 SHIPPED / DELIVERED
kubectl -n shop scale deploy/fulfillment --replicas=1

# 手动催一单，不用等 ticker（演示时很省时间）
curl -s -X POST http://localhost/fulfill/shipments/<orderId>/advance

# 履约服务回写被 api 拒绝会怎样：把 INTERNAL_TOKEN 改错，
# 观察日志里的 advance_rolled_back（状态回滚、5s 后重试）与
# shop_shipment_callbacks_total{result="error"}

# 状态机守卫：直接给一个 CREATED 订单回写 SHIPPED，必须 409
curl -s -X POST http://localhost/api/internal/shipments \
  -H 'Content-Type: application/json' \
  -H "X-Internal-Token: $(kubectl -n shop get secret shop-secret -o jsonpath='{.data.internal-token}' | base64 -d)" \
  -d '{"orderId":<CREATED 的订单>,"status":"SHIPPED","trackingNo":"X1"}'   # → 409

# 越权：带别人的 token（或干脆不带）查一个有归属的订单 → 404 而不是 403
curl -s http://localhost/api/orders/<别人的订单号> -i | head -1
```

## 已知限制

- ~~`readyz` 把 Redis 当硬依赖~~：已改成 PG 硬 / Redis 软（见上文 API 表下的说明）。顺带修了同一件事的另一半 —— `cache.New` 原来在启动时 ping 失败就 `os.Exit(1)`，那等于「运维重启 Redis 的 30 秒里 Pod 反复 CrashLoop，退避越起越慢，缓存早就恢复了服务还没起来」
- ~~`MaxOpenConns` / `MaxIdleConns` 硬编码~~：已进 `shop-config`（`DB_MAX_OPEN_CONNS` / `DB_MAX_IDLE_CONNS` / `DB_CONN_MAX_LIFETIME`），idle 默认等于 open
- ~~DSN 没设 `statement_timeout` / `application_name`~~：已在 `config.withDBParams` 里按服务注入（迁移的 DDL 用 `SET LOCAL statement_timeout = 0` 豁免，且不会随连接回池泄漏）
- ~~`CreateOrder` 提交后又回查三次拿详情~~：已改成 `RETURNING id, created_at, updated_at` + 用内存里的明细直接构造
- 单副本、无 HPA/PDB、无 NetworkPolicy
- `Idempotency-Key` 仍由前端生成，服务端不校验格式
- ~~**存量订单不会自动发货**~~：已修。`backend/cmd/backfill` 给「PAID 且无 `order.paid` 事件」的订单补 outbox，实测 31 张单全部补发到位。做法见「支付之后：从 PAID 到 DELIVERED」小节
- ~~没有「我的订单」列表接口~~：已加 `GET /api/orders`，只按 token 里的 uid 查，明细和状态历史用两条 `= ANY($1)` 批量补齐，不做 N+1。仍然没有分页——订单多了要改游标，offset 会漏单
- JWT 无法吊销：纯无状态，退出登录只是前端删 token。要立刻失效得上黑名单或 RS256 + 短 access + refresh
- 前端 token 存 `localStorage`，XSS 可直接读走
- 支付镜像是本地 `docker build` 的 `local2` tag，没走 CI；集群重置后要重新 build 并导入节点（`docker save | docker exec -i desktop-worker ctr -n k8s.io images import -`）
- `backend:fulfill1` / `frontend:fulfill1` / `fulfillment:local1` 同样是本地镜像：Docker Hub 不可达时拉不到 `golang:1.24-alpine` 和 `nginx:1.27-alpine`，于是宿主机交叉编译后借现成镜像的运行层重新打包（见下文「Docker Hub 挂了怎么出镜像」）
- **同 tag 重建镜像后必须重新导入节点**：`imagePullPolicy: IfNotPresent` + containerd 里有同名 tag 的旧 manifest 时，`rollout restart` 用的还是旧镜像，症状是「代码明明改了却没生效」。要么换 tag，要么 `docker save | ctr import`
- worker 调支付是**同步 HTTP**，3s 超时；支付服务慢会直接压到消费者的消费速率（prefetch=5 时最多 5 个并发卡在等）
- Alloy 的 River 配置写 `#` 注释会**静默**失效（reload 报错但继续跑旧配置，ArgoCD 依旧 Synced/Healthy）；排查方式是看 alloy 日志里的 `failed to reload config`
- ~~没有 tracing~~：已接 OTel + Tempo，一单跨 5 个进程是一条 trace。Go 侧的 SQL 也采上了（`otelsql`，span 名是 `SELECT orders` 这种「操作 + 表名」）。剩下的缺口是**没有 Collector**（应用直连 Tempo，将来要加尾采样/脱敏得在前面插一层），以及物流推进是独立 trace（按 `order.id` 关联，不硬挂在支付那条上）
- `/api/internal/*` 和 `/fulfill/*` 都暴露在同一个 Ingress 后面，只靠 token 和应用层挡；生产应该在网关层加路径拒绝
- **连接池没有可观测性**：池大小可调了，但 `sql.DBStats` 没导出成 Prometheus 指标，所以「压测时到底撞没撞到 10 个连接」还是看不出来。要接 `otelsql.RegisterDBStatsMetrics` 就得再架一个 otel→Prometheus 的桥，暂时没做
- **DLQ 有告警有工具，但没有自动重放**：`ShopDlqNotEmpty` 响了之后仍然要人跑 `/app/replay`。也没有保留期 —— DLQ 里的消息不会过期，攒几年也没人管。真要做自动重放，得先能区分「可重试的临时故障」和「永远好不了的数据问题」，现在没有这个判断依据
- **重放不会复活已取消的订单**：订单在重试耗尽时已被补偿成 `CANCELLED`，重投回去 worker 会 `skip_duplicate_event`。这是对的（消息重放 ≠ 业务重做），但意味着「从 DLQ 捞回来」只对「消息丢了、订单还活着」的情况有效
- **重试队列本身没有告警**：`ShopQueueBacklog` 只看主队列。重试队列积压（下游持续失败但还没放弃）其实是「故障正在发生」的更早信号

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
