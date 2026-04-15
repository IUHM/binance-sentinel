FROM node:20-alpine AS build

RUN corepack enable

WORKDIR /app

COPY frontend/package.json ./package.json
COPY frontend/pnpm-lock.yaml ./pnpm-lock.yaml
COPY frontend/vite.config.js ./vite.config.js
COPY frontend/index.html ./index.html
COPY frontend/src ./src

ARG VITE_API_BASE_URL=
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}

RUN pnpm install --frozen-lockfile
RUN pnpm build

FROM nginx:1.27-alpine

ENV API_UPSTREAM=api:8000

COPY frontend/nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
