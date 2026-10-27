FROM node:22-alpine
RUN apk add --no-cache openssl

EXPOSE 3000

WORKDIR /app

ENV NODE_ENV=production

COPY package.json package-lock.json* ./

RUN npm ci --include=dev

COPY . .

RUN DATABASE_URL=file:/tmp/build.sqlite npx prisma generate \
    && npm run build \
    && npm prune --omit=dev \
    && npm cache clean --force \
    && mkdir -p /app/storage/db /app/var/imports \
    && chown -R node:node /app/storage /app/var

USER node

CMD ["npm", "run", "start"]
