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
| `deploy/` | Kustomize 清单：namespace、前后端 Deployment（各 1 副本）、Ingress |
| `argocd/` | ArgoCD `Application` 定义 |
| `.github/workflows/ci.yaml` | 构建推送 + 回写镜像 tag |

> 数据库不跑在集群里。后端连宿主机上的 Homebrew PostgreSQL 18（`host.docker.internal:5432/shop`，库 `shop`，角色 `shop/shop`），由 Docker Desktop 的 DNS 解析到 `192.168.65.254`。本地学习用，单副本、无高可用、无备份。

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
