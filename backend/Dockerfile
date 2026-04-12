FROM node:20-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY src ./src
COPY uploads ./uploads

ENV NODE_ENV=production
EXPOSE 4000

CMD ["npm", "start"]
