# syntax=docker/dockerfile:1
# 币安极速多IP监控与自动跟单系统 v3.1 — 运行镜像
# 只装运行依赖与代码；配置 / 数据库 / keyfile 全部通过 volume 持久化到宿主机 data/。
FROM python:3.11-slim

# 北京时间日志；不生成 .pyc；stdout/stderr 不缓冲，docker logs 可实时看到日志。
ENV TZ=Asia/Shanghai \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# ca-certificates：HTTPS 证书校验；tzdata：让 TZ=Asia/Shanghai 真正生效。
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

# 先只复制依赖清单并安装：requirements.txt 未变化时可命中 Docker 层缓存，重建秒级完成。
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 再复制代码目录。data/ 绝不复制进镜像，运行时由 docker-compose 挂载 ./data:/app/data。
COPY backend/ ./backend/
COPY frontend/ ./frontend/
# v3.3：TG 采集模块（同一个镜像跑第二个容器）
COPY tg/ ./tg/

# 容器内占位目录；实际内容由宿主机 ./data 挂载覆盖。
RUN mkdir -p /app/data

EXPOSE 8080

# 默认监听 8080；docker-compose 会通过 COPYBOT_PORT 变量覆盖此启动命令。
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8080"]
