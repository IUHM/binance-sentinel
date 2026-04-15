# 币安永续异动哨兵

这是一个只监控 `Binance USDT 永续` 的实时告警系统，目标就是两件事：

- 费率异常上涨或下跌时，尽快告警
- 大额挂单和最新催化情报出现时，尽快告诉你原因

前端是中文科技感交易台，后端负责实时采集、评分、去重、情报归因和 Telegram 推送。

## 当前能力

- 自动拉取 Binance `USDT 永续` 合约池
- 监控资金费率异常、持仓价值变化和强平压力
- 扫描大额挂单候选，并做二次深度确认
- 聚合 `Binance 官方公告 + 主流媒体 RSS + X 白名单 RSS 代理`
- WebSocket 实时推送到网页端
- 支持 Telegram 告警
- 前端支持费率热榜、费率走势图、挂单雷达、告警流、情报流和规则面板

## 目录结构

```text
binance-sentinel/
├─ backend/
├─ frontend/
├─ docker-compose.yml
├─ deploy.sh
├─ .env.example
└─ README.md
```

## 很重要

如果你在中国大陆本机直接访问 Binance 合约接口，通常会遇到 `HTTP 451`，这不是代码问题，是网络环境问题。

正式运行请放到能正常访问 Binance Futures 的海外 VPS 上。

## 一键部署到海外 VPS

### 1. 准备环境

在 Ubuntu 或 Debian 类 VPS 上安装 Docker 和 Compose。

### 2. 上传项目后执行

```bash
cd /root/binance-sentinel
cp .env.example .env
vim .env
chmod +x deploy.sh
./deploy.sh
```

### 3. 你至少要改这几个配置

- `POSTGRES_PASSWORD`
- `DATABASE_URL`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

如果前后端同域部署，`VITE_API_BASE_URL` 保持空白就行，前端会自动走当前域名下的 `/api` 和 `/ws`。

## 默认访问地址

- 网页：`http://你的VPS公网IP`
- 健康检查：`http://你的VPS公网IP/healthz`

## 本地开发

### 后端 API

```powershell
cd D:\94196\Documents\binance-sentinel
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .[dev]
uvicorn backend.app:app --host 0.0.0.0 --port 8000 --reload
```

### Worker

```powershell
cd D:\94196\Documents\binance-sentinel
.\.venv\Scripts\Activate.ps1
python -m backend.worker
```

### 前端

```powershell
cd D:\94196\Documents\binance-sentinel\frontend
pnpm install
pnpm dev
```

## Docker Compose 服务说明

- `web`：Nginx 托管前端静态资源，并反代 `/api` 和 `/ws`
- `api`：FastAPI 接口层
- `worker`：币安实时采集、情报采集、异常识别和告警发送
- `postgres`：主数据库
- `redis`：后续扩展和热状态缓存预留

## 关键环境变量

- `APP_PORT`：网页暴露端口，默认 `80`
- `DATABASE_URL`：数据库连接串
- `BINANCE_REST_BASE_URL`
- `BINANCE_WS_BASE_URL`
- `FUNDING_HISTORY_SAMPLE_SECONDS`：费率走势采样间隔，默认 `120`
- `FUNDING_HISTORY_RETENTION_HOURS`：费率走势图保留时长，默认 `72`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `OFFICIAL_RSS_URLS`
- `MEDIA_RSS_URLS`
- `X_SOURCE_TEMPLATE`
- `X_WHITELIST`

## 测试

```powershell
cd D:\94196\Documents\binance-sentinel
pytest
```

当前测试覆盖：

- 极端 funding rate 是否能被正确打分
- 持续性大额挂单是否能被识别为墙单

## 备注

- 第一版是单用户私有部署，不做多租户
- Telegram 不配置也能运行，但不会向 TG 发送告警
- 网页看板是中文，Telegram 告警也已改成中文

## 不想折腾 VPS 的最省事方案

仓库根目录已经带了 `render.yaml`，可以直接走 Render 蓝图部署。

你只需要做这件事：

1. 注册并登录 Render
2. 导入这个仓库
3. 让 Render 读取根目录的 `render.yaml`
4. 点击创建

这样它会自动帮你起：

- 前端公网网页
- API 服务
- 后台 worker
- Postgres 数据库

等你后面愿意补 `TELEGRAM_BOT_TOKEN` 和 `TELEGRAM_CHAT_ID` 时，再到 Render 后台把这两个环境变量填进去就行。
