"""极简支付服务（FastAPI）。

定位：模拟第三方支付网关，只做一件事——对某个订单一次性扣款，并且幂等。

真实网关（Stripe / Adyen）的四个关键行为这里都有：
  1. 幂等：同一个 order_id 重复调用拿到同一个结果，绝不重复扣款
  2. 持久化：扣款结果是落库的事实，进程重启不丢
  3. 可回查：调用方可以拿 order_id 查状态（对账用）
  4. 拒付是业务结果，不是 500 —— 支付失败要能被调用方正常解析

环境变量：
  DATABASE_URL          PG 连接串，默认指向本地 shop 库
  PAYMENT_FAILURE_RATE  0~1，模拟拒付比例，默认 0（全成功）
  LOG_LEVEL             debug/info/warn/error，默认 info
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

SERVICE = "payment"
VERSION = os.getenv("APP_VERSION", "dev")


# --------------------------------------------------------------------------
# 日志：输出 JSON 到 stdout，字段和 Go 侧的 slog 对齐（level / msg），
# 这样 Alloy 的 stage.json 不用改就能把 level 提成 label
# --------------------------------------------------------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname.lower(),
            "msg": record.getMessage(),
            "service": SERVICE,
            "version": VERSION,
            "logger": record.name,
        }
        for key, value in getattr(record, "extra_fields", {}).items():
            payload[key] = value
        if record.exc_info:
            payload["err"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging() -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "info").upper()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(getattr(logging, level, logging.INFO))
    # uvicorn 自己的 access log 是纯文本，会污染 JSON 流，关掉（我们有自己的业务日志）
    logging.getLogger("uvicorn.access").disabled = True
    return logging.getLogger(SERVICE)


log = setup_logging()


# --------------------------------------------------------------------------
# 指标
# --------------------------------------------------------------------------
CHARGES = Counter("payment_charges_total", "扣款请求数", ["result"])
CHARGE_LATENCY = Histogram(
    "payment_charge_duration_seconds",
    "扣款耗时",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)


# --------------------------------------------------------------------------
# 数据库
# --------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
    order_id       text PRIMARY KEY,
    amount_cents   bigint      NOT NULL CHECK (amount_cents > 0),
    currency       text        NOT NULL DEFAULT 'CNY',
    status         text        NOT NULL CHECK (status IN ('succeeded', 'failed')),
    payment_id     text        NOT NULL,
    failure_reason text,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS payments_created_at_idx ON payments (created_at DESC);
"""

pool: ConnectionPool | None = None


def get_pool() -> ConnectionPool:
    if pool is None:  # pragma: no cover - 正常流程里 lifespan 已经建好了
        raise RuntimeError("数据库连接池未初始化")
    return pool


def init_pool() -> ConnectionPool:
    """建池 + 建表。PG 刚起来时可能连不上，重试几次而不是直接崩。"""
    dsn = os.getenv(
        "DATABASE_URL",
        "postgresql://shop:shop@localhost:5432/shop?sslmode=disable",
    )
    last_err: Exception | None = None
    for attempt in range(1, 11):
        try:
            p = ConnectionPool(dsn, min_size=1, max_size=5, timeout=5)
            p.wait(timeout=10)
            with p.connection() as conn:
                conn.execute(SCHEMA)
            log.info("db_ready", extra={"extra_fields": {"attempt": attempt}})
            return p
        except Exception as exc:  # noqa: BLE001 - 连不上就重试，最后一次抛出
            last_err = exc
            log.warning("db_connect_retry", extra={"extra_fields": {"attempt": attempt, "err": str(exc)}})
            time.sleep(2)
    raise RuntimeError(f"无法连接数据库: {last_err}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = init_pool()
    log.info("service_started", extra={"extra_fields": {"svc": SERVICE, "version": VERSION}})
    try:
        yield
    finally:
        pool.close()
        log.info("service_stopped")


app = FastAPI(title="shop payment service", version=VERSION, lifespan=lifespan)


# --------------------------------------------------------------------------
# 协议
# --------------------------------------------------------------------------
class ChargeRequest(BaseModel):
    order_id: str = Field(min_length=1, max_length=64)
    amount_cents: int = Field(gt=0, le=100_000_00)
    currency: str = Field(default="CNY", min_length=3, max_length=3)


class ChargeResponse(BaseModel):
    order_id: str
    status: str
    payment_id: str
    amount_cents: int
    currency: str
    failure_reason: str | None = None
    replayed: bool = False


def decide(order_id: str, failure_rate: float) -> tuple[str, str | None]:
    """按 order_id 的哈希决定成功/失败：同一个订单重放时结论必须一致。"""
    if failure_rate <= 0:
        return "succeeded", None
    bucket = int(hashlib.sha256(order_id.encode()).hexdigest()[:8], 16) % 10_000
    if bucket < failure_rate * 10_000:
        return "failed", "card_declined"
    return "succeeded", None


# --------------------------------------------------------------------------
# 路由
# --------------------------------------------------------------------------
@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "service": SERVICE, "version": VERSION}


@app.get("/readyz")
def readyz() -> Response:
    try:
        with get_pool().connection() as conn:
            conn.execute("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        log.error("db_unreachable", extra={"extra_fields": {"err": str(exc)}})
        return Response(content='{"status":"degraded","db":"down"}', status_code=503, media_type="application/json")
    return Response(content='{"status":"ok"}', status_code=200, media_type="application/json")


@app.post("/charge", response_model=ChargeResponse)
def charge(req: ChargeRequest) -> ChargeResponse:
    started = time.perf_counter()
    failure_rate = float(os.getenv("PAYMENT_FAILURE_RATE", "0"))

    status, reason = decide(req.order_id, failure_rate)
    payment_id = "pay_" + hashlib.sha256(req.order_id.encode()).hexdigest()[:20]

    with get_pool().connection() as conn:
        # 幂等核心：主键冲突就什么都不做，随后把原来那条读出来返回。
        # 必须先插入再判断，而不是先查后插 —— 后者在并发下会双扣。
        row = conn.execute(
            """
            INSERT INTO payments (order_id, amount_cents, currency, status, payment_id, failure_reason)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            RETURNING order_id, amount_cents, currency, status, payment_id, failure_reason
            """,
            (req.order_id, req.amount_cents, req.currency, status, payment_id, reason),
        ).fetchone()
        replayed = row is None
        if replayed:
            row = conn.execute(
                """
                SELECT order_id, amount_cents, currency, status, payment_id, failure_reason
                FROM payments WHERE order_id = %s
                """,
                (req.order_id,),
            ).fetchone()

    order_id, amount_cents, currency, st, pid, fail = row
    elapsed = time.perf_counter() - started
    CHARGE_LATENCY.observe(elapsed)
    CHARGES.labels(result="replayed" if replayed else st).inc()
    log.info(
        "charge",
        extra={
            "extra_fields": {
                "order_id": order_id,
                "result": st,
                "replayed": replayed,
                "amount_cents": amount_cents,
                "dur_ms": round(elapsed * 1000, 2),
            }
        },
    )

    return ChargeResponse(
        order_id=order_id,
        status=st,
        payment_id=pid,
        amount_cents=amount_cents,
        currency=currency,
        failure_reason=fail,
        replayed=replayed,
    )


@app.get("/payments/{order_id}", response_model=ChargeResponse)
def get_payment(order_id: str) -> ChargeResponse:
    with get_pool().connection() as conn:
        row = conn.execute(
            """
            SELECT order_id, amount_cents, currency, status, payment_id, failure_reason
            FROM payments WHERE order_id = %s
            """,
            (order_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="payment not found")
    return ChargeResponse(
        order_id=row[0],
        amount_cents=row[1],
        currency=row[2],
        status=row[3],
        payment_id=row[4],
        failure_reason=row[5],
    )


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
