FROM node:20-alpine AS build

WORKDIR /app

COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ ./

ARG VITE_USE_SAME_ORIGIN_API=true
ENV VITE_USE_SAME_ORIGIN_API=${VITE_USE_SAME_ORIGIN_API}

RUN npm run build

FROM nginx:1.27-alpine

COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 2026

CMD ["nginx", "-g", "daemon off;"]
