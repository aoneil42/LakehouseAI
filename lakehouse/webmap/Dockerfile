## Stage 1: Build
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci
COPY . .
RUN npm run build

## Stage 2: Serve
FROM nginx:alpine
ARG BASEMAP_CONFIG=basemaps.json
RUN apk add --no-cache curl
COPY --from=build /app/dist /usr/share/nginx/html
COPY public/${BASEMAP_CONFIG} /usr/share/nginx/html/basemaps.json
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
