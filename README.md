# shop-gitops

前后端分离的最小电商骨架，用 **GitHub Actions 构建 + ArgoCD 交付**，在本地 Docker Desktop Kubernetes 上跑通完整的 GitOps 闭环。

```
改代码 push
   │
   ├─ GitHub Actions  ── build backend/frontend → push ghcr.io → 改 deploy/ 里的 tag 并提交
   │
   └─ ArgoCD          ── 发现新 commit → 对比 live state → 自动 sync 到集群
```

## 组成

| 路径 | 内容 |
|---|---|
| `frontend/` | Vite + 原生 JS，构建成静态 `dist/`，由 nginx 提供 |
| `backend/` | Go 单二进制，`/api/products`、`/api/cart`，连 PostgreSQL |
| `deploy/` | Kustomize 清单：namespace、postgres StatefulSet、前后端 Deployment、Ingress |
| `argocd/` | ArgoCD `Application` 定义 |
| `.github/workflows/ci.yaml` | 构建推送 + 回写镜像 tag |

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/hello` | 返回 Pod 名与版本，验证分流 |
| GET | `/api/products` | 商品列表（读 PG） |
| POST | `/api/cart` | 下单扣库存（事务内条件更新） |
| GET | `/healthz` | 存活探针，不查 DB |
| GET | `/readyz` | 就绪探针，查 DB，失败则摘流量 |

> `healthz` 与 `readyz` 故意分开：DB 挂了应该停止接流量，而不是被 liveness 反复重启。

## 本地访问

```bash
kubectl port-forward -n argocd svc/argocd-server 8080:80
open http://localhost:8080          # admin / 见 argocd-initial-admin-secret
```

集群里走 Ingress：

```bash
curl http://shop.localhost/            # 前端
curl http://shop.localhost/api/products
```

## 关键设计

- **同域反代**：前端只写相对路径 `/api/...`，CORS 问题不存在
- **镜像用 commit sha**：不用 `latest`，回滚有确定性，ArgoCD 能判断 Git 是否真的变了
- **DB 在集群内**：StatefulSet + 2Gi PVC，`replication_factor` 无所谓，目的是让 demo 自包含
- **selfHeal**：手工 `kubectl edit` 会被 ArgoCD 自动拉回 Git 的状态

## 需要手工做一次的

GitHub Packages 默认私有，要让集群匿名拉镜像，需把两个 package 设为 public：

```
https://github.com/users/Jeffrey-Xu/packages/container/shop-gitops%2Fbackend/settings
https://github.com/users/Jeffrey-Xu/packages/container/shop-gitops%2Ffrontend/settings
```

或者建 `imagePullSecret` 走认证拉取。
