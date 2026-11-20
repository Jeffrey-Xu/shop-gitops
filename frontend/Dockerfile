FROM node:22-alpine AS build
WORKDIR /app

COPY package*.json ./
RUN npm install --no-audit --no-fund

COPY . .
ARG BUILD_ID=dev
ENV BUILD_ID=${BUILD_ID}
RUN npm run build && sed -i "s/__BUILD__/${BUILD_ID}/g" dist/index.html

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
