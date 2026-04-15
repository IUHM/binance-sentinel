FROM node:20-alpine AS frontend-build

RUN corepack enable

WORKDIR /frontend

COPY frontend/package.json ./package.json
COPY frontend/pnpm-lock.yaml ./pnpm-lock.yaml
COPY frontend/vite.config.js ./vite.config.js
COPY frontend/index.html ./index.html
COPY frontend/src ./src

ARG VITE_API_BASE_URL=
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}

RUN pnpm install --frozen-lockfile
RUN pnpm build

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8000
ENV FRONTEND_DIST_DIR=/app/frontend_dist

WORKDIR /app

COPY pyproject.toml README.md ./
COPY backend ./backend
COPY --from=frontend-build /frontend/dist ./frontend_dist

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

EXPOSE 8000

CMD uvicorn backend.app:app --host 0.0.0.0 --port ${PORT:-8000}
