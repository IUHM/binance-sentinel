#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "未检测到 docker，请先在 VPS 安装 Docker。"
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  echo "未检测到 docker compose，请先安装 Docker Compose。"
  exit 1
fi

if [ ! -f .env ]; then
  cp .env.example .env
  echo "已生成 .env，请先把 POSTGRES_PASSWORD 和 Telegram 配置改掉，再重新执行。"
  exit 0
fi

set -a
. ./.env
set +a

if [ "${POSTGRES_PASSWORD:-}" = "change_me_now" ] || [ -z "${POSTGRES_PASSWORD:-}" ]; then
  echo "请先修改 .env 里的 POSTGRES_PASSWORD，再执行部署。"
  exit 1
fi

if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
  echo "警告：Telegram 还没配置，系统可以启动，但不会向 TG 推送告警。"
fi

echo "开始构建并启动 Binance Sentinel..."
$COMPOSE_CMD up -d --build

echo
echo "当前容器状态："
$COMPOSE_CMD ps

echo
echo "部署完成。默认访问地址："
echo "http://<你的VPS公网IP>:${APP_PORT:-80}"
echo
echo "如需查看日志："
echo "$COMPOSE_CMD logs -f web api worker"
