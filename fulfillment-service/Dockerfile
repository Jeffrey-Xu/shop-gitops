# Node 22 LTS：Fastify 5 要求 Node >= 20，原生 fetch 也在这条线上
FROM node:22-alpine

WORKDIR /app

# 先只拷依赖清单，让 npm ci 这一层能被缓存住 ——
# 改业务代码时不必重新装一遍 node_modules
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY src ./src

ENV NODE_ENV=production
# 不以 root 跑：镜像里 node 用户是内置的
USER node

EXPOSE 8080

# 直接 exec node，不要用 sh -c / npm start：
# 那样 SIGTERM 会打在 shell 上，node 收不到，优雅退出就失效了
CMD ["node", "src/server.js"]
