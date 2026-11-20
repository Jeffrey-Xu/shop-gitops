FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# 先装依赖再拷代码，改代码时不会把依赖层也失效
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app

# 非 root 运行；1000 在 slim 镜像里已经存在
RUN useradd --uid 1000 --create-home appuser || true
USER 1000

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "info"]
